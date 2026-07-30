#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "http/httpfs.hpp"
#include "http/httpfs_client.hpp"
#include "crypto.hpp"
#include "s3/s3_request.hpp"
#include "s3/s3fs.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <array>
#include <new>

namespace duckdb {

namespace {

static void RunFullGetErrorBodyScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::FULL_GET;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false, false);

	S3TestHelper::RequireQueryOk(con, "SET force_download=true");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	string error;
	try {
		auto &fs = FileSystem::GetFileSystem(*con.context);
		fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		error = ex.what();
	}
	INFO(error);
	REQUIRE(error.find("AccessDenied: stale credentials") != string::npos);
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", S3TestHelper::STALE_KEY_ID, 403));
	REQUIRE_FALSE(S3TestHelper::HasRequestWithKey(observations, S3TestHelper::FRESH_KEY_ID));
	S3TestHelper::AssertNoRefresh(test_id);
}

static void RunChunkedFullGetScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.full_get.chunked = true;
	auto object_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET force_download=true");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ);
	string result(object_data.size(), '\0');
	handle->Read(QueryContext(*con.context), &result[0], result.size(), 0);
	REQUIRE(result == object_data);
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", S3TestHelper::STALE_KEY_ID, 200));
	S3TestHelper::AssertNoRefresh(test_id);
}

static void RunS3HeaderScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.auth.stale_key_id = "NEVER_STALE";
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);
	S3TestHelper::RequireQueryOk(con,
	                             StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	S3TestHelper::RequireQueryOk(con, "SET httpfs_connection_caching=false");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET s3_headers (
	TYPE S3,
	SCOPE '%s',
	KEY_ID 'FRESH_KEY',
	SECRET 'S3TestHelper::FRESH_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path',
	EXTRA_HTTP_HEADERS MAP {'X-HTTPFS-Session': 'present'}
))",
	                                                     S3TestHelper::S3_PATH, server.Endpoint()));

	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	{
		auto handle = fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
		REQUIRE(handle);
	}
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE_FALSE(observations.empty());
	for (const auto &observation : observations) {
		REQUIRE(observation.user_agent_count == 1);
		REQUIRE_FALSE(observation.user_agent.empty());
		REQUIRE(observation.session_header_count == 1);
		REQUIRE(observation.session_header == "present");
	}
}

static void RunS3RegionRedirectScenario(const string &client_implementation, bool list_request) {
	MockS3ServerConfig config;
	config.auth.stale_key_id = "NEVER_STALE";
	config.auth.required_region = "us-east-1";
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);
	S3TestHelper::RequireQueryOk(con,
	                             StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	S3TestHelper::RequireQueryOk(con, "SET httpfs_connection_caching=false");
	S3TestHelper::RequireQueryOk(con, "SET enable_logging=true");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET s3_region_redirect (
	TYPE S3,
	SCOPE 's3://refresh-bucket/',
	KEY_ID 'FRESH_KEY',
	SECRET 'S3TestHelper::FRESH_SECRET',
	REGION 'eu-west-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
	                                                     server.Endpoint()));

	if (list_request) {
		auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/*.bin')");
		REQUIRE(result);
		INFO((result->HasError() ? result->GetError() : string()));
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->RowCount() == 1);
	} else {
		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto &fs = FileSystem::GetFileSystem(*con.context);
		{
			auto handle =
			    fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
			REQUIRE(handle);
		}
		S3TestHelper::RequireQueryOk(con, "COMMIT");
	}

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	bool saw_redirect = false;
	bool saw_success = false;
	for (const auto &observation : observations) {
		if (observation.status == 301 && observation.region == "eu-west-1") {
			saw_redirect = true;
		}
		if (observation.status == 200 && observation.region == "us-east-1") {
			saw_success = true;
		}
	}
	REQUIRE(saw_redirect);
	REQUIRE(saw_success);

	auto logs = con.Query("SELECT count(*) FROM duckdb_logs WHERE message LIKE '%incorrect region%'");
	REQUIRE(logs);
	INFO((logs->HasError() ? logs->GetError() : string()));
	REQUIRE_FALSE(logs->HasError());
	REQUIRE(logs->GetValue(0, 0).GetValue<int64_t>() == 1);
}

} // namespace

TEST_CASE("S3 request signing remains deterministic", "[httpfs][s3][signing]") {
	::AESStateSSLFactory encryption_util;
	S3AuthParams auth_params;
	auth_params.region = "us-east-1";
	auth_params.access_key_id = "AKIAIOSFODNN7EXAMPLE";
	auth_params.secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

	auto headers = S3RequestUtil::CreateHeader(encryption_util, "/test.txt", "", "examplebucket.s3.amazonaws.com", "s3",
	                                           "GET", auth_params, "20130524", "20130524T000000Z");

	REQUIRE(headers.GetHeaderValue("Host") == "examplebucket.s3.amazonaws.com");
	REQUIRE(headers.GetHeaderValue("x-amz-content-sha256") ==
	        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
	REQUIRE(headers.GetHeaderValue("Authorization") ==
	        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
	        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
	        "Signature=df548e2ce037944d03f3e68682813b093763996d597cf890ca3d9037fd231eb4");
}

TEST_CASE("S3 request signing includes optional headers", "[httpfs][s3][signing]") {
	::AESStateSSLFactory encryption_util;
	S3AuthParams auth_params;
	auth_params.region = "eu-west-1";
	auth_params.access_key_id = "key";
	auth_params.secret_access_key = "secret";
	auth_params.session_token = "token";
	auth_params.kms_key_id = "kms-key";
	auth_params.requester_pays = true;

	auto headers =
	    S3RequestUtil::CreateHeader(encryption_util, "/bucket/key", "", "bucket.example.com", "s3", "PUT", auth_params,
	                                "20260730", "20260730T120000Z", "", "application/octet-stream", "content-md5");

	REQUIRE(headers.GetHeaderValue("x-amz-security-token") == "token");
	REQUIRE(headers.GetHeaderValue("x-amz-request-payer") == "requester");
	REQUIRE(headers.GetHeaderValue("x-amz-server-side-encryption") == "aws:kms");
	REQUIRE(headers.GetHeaderValue("x-amz-server-side-encryption-aws-kms-key-id") == "kms-key");
	REQUIRE(headers.GetHeaderValue("Authorization").find("content-md5;content-type;host;") != string::npos);
}

TEST_CASE("HTTPFS preserves S3 error bodies for streamed GETs", "[httpfs][s3][errors]") {
	SECTION("httplib") {
		RunFullGetErrorBodyScenario("httplib");
	}
	SECTION("curl") {
		RunFullGetErrorBodyScenario("curl");
	}
}

TEST_CASE("HTTPFS streams full GETs without Content-Length", "[httpfs][s3][streaming]") {
	SECTION("httplib") {
		RunChunkedFullGetScenario("httplib");
	}
	SECTION("curl") {
		RunChunkedFullGetScenario("curl");
	}
}

TEST_CASE("HTTPFS initializes and clones parameters without a configured proxy", "[httpfs][s3][params]") {
	alignas(HTTPFSParams) std::array<uint8_t, sizeof(HTTPFSParams)> storage;
	storage.fill(0xA5);

	HTTPFSUtil http_util;
	auto params = new (storage.data()) HTTPFSParams(http_util);
	REQUIRE(params->http_proxy.empty());
	REQUIRE(params->http_proxy_port == 0);
	auto clone = params->Clone();
	params->~HTTPFSParams();

	auto &cloned_params = clone->Cast<HTTPFSParams>();
	REQUIRE(cloned_params.http_proxy.empty());
	REQUIRE(cloned_params.http_proxy_port == 0);
}

TEST_CASE("HTTP request sessions merge configured headers once", "[httpfs][request-session][headers]") {
	SECTION("httplib") {
		RunS3HeaderScenario("httplib");
	}
	SECTION("curl") {
		RunS3HeaderScenario("curl");
	}
}

TEST_CASE("S3 request sessions publish region redirects", "[httpfs][request-session][s3][region]") {
	SECTION("httplib HEAD") {
		RunS3RegionRedirectScenario("httplib", false);
	}
	SECTION("curl HEAD") {
		RunS3RegionRedirectScenario("curl", false);
	}
	SECTION("httplib LIST") {
		RunS3RegionRedirectScenario("httplib", true);
	}
	SECTION("curl LIST") {
		RunS3RegionRedirectScenario("curl", true);
	}
}

} // namespace duckdb
