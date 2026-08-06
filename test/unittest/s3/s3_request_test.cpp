#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "http/httpfs.hpp"
#include "http/httpfs_client.hpp"
#include "crypto.hpp"
#include "s3/s3_provider.hpp"
#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3fs.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/secret/secret.hpp"

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

static void ConfigureGCSBearer(Connection &con, MockS3Server &server, const string &client_implementation) {
	S3TestHelper::RequireQueryOk(con,
	                             StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	S3TestHelper::RequireQueryOk(con, "SET httpfs_connection_caching=false");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET gcs_bearer (
	TYPE GCS,
	SCOPE 'gcs://refresh-bucket/',
	BEARER_TOKEN 'gcs-test-token',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
	                                                     server.Endpoint()));
}

static void RunGCSBearerRequestScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.auth.stale_key_id = "NEVER_STALE";
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);
	ConfigureGCSBearer(con, server, client_implementation);

	const string gcs_path = "gcs://refresh-bucket/object.bin";
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(gcs_path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(handle);

	auto list_result = con.Query("SELECT file FROM glob('gcs://refresh-bucket/*.bin')");
	REQUIRE(list_result);
	INFO((list_result->HasError() ? list_result->GetError() : string()));
	REQUIRE_FALSE(list_result->HasError());
	REQUIRE(list_result->RowCount() == 1);

	fs.RemoveFiles({gcs_path});
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	bool saw_object = false;
	bool saw_list = false;
	bool saw_bulk_delete = false;
	for (const auto &observation : observations) {
		if (observation.authorization != "Bearer gcs-test-token") {
			continue;
		}
		saw_object |= observation.method == "HEAD" && observation.target.find("object.bin") != string::npos;
		saw_list |= observation.method == "GET" && observation.target.find("list-type=2") != string::npos;
		saw_bulk_delete |= observation.method == "POST" && observation.target.find("delete") != string::npos;
	}
	REQUIRE(saw_object);
	REQUIRE(saw_list);
	REQUIRE(saw_bulk_delete);
}

static void RunGCSListAuthErrorScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.auth.stale_authorization = "Bearer gcs-test-token";
	config.auth.refresh_target = MockS3RefreshTarget::LIST_OBJECTS_GET;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);
	ConfigureGCSBearer(con, server, client_implementation);

	auto result = con.Query("SELECT file FROM glob('gcs://refresh-bucket/*.bin')");
	REQUIRE(result);
	REQUIRE(result->HasError());
	INFO(result->GetError());
	REQUIRE(result->GetError().find("Authentication Failure - GCS authentication failed") != string::npos);
}

static S3AuthParams ReadSecretAuthParams(Connection &con, KeyValueSecret &secret) {
	ClientContextFileOpener opener(*con.context);
	S3KeyValueReader secret_reader {KeyValueSecretReader(secret, opener)};
	return S3AuthParams::ReadFrom(secret_reader, "s3://bucket/key");
}

} // namespace

TEST_CASE("S3 provider policy resolves URL aliases and default scopes", "[httpfs][s3][provider]") {
	for (const auto &entry : {pair<string, S3ProviderType> {"s3://bucket/key", S3ProviderType::S3},
	                          pair<string, S3ProviderType> {"S3A://bucket/key", S3ProviderType::S3},
	                          pair<string, S3ProviderType> {"s3n://bucket/key", S3ProviderType::S3},
	                          pair<string, S3ProviderType> {"gcs://bucket/key", S3ProviderType::GCS},
	                          pair<string, S3ProviderType> {"GS://bucket/key", S3ProviderType::GCS},
	                          pair<string, S3ProviderType> {"r2://bucket/key", S3ProviderType::R2}}) {
		auto provider_match = S3Provider::MatchUrl(entry.first);
		REQUIRE(provider_match.type == entry.second);
	}
	REQUIRE_FALSE(S3Provider::TryMatchUrl("https://bucket/key"));
	REQUIRE_FALSE(S3Provider::TryMatchUrl("aws://bucket/key"));
	REQUIRE(S3Provider::DefaultSecretScope("s3") == vector<string> {"s3://", "s3n://", "s3a://"});
	REQUIRE(S3Provider::DefaultSecretScope("gcs") == vector<string> {"gcs://", "gs://"});
	REQUIRE(S3Provider::DefaultSecretScope("r2") == vector<string> {"r2://"});
	REQUIRE(S3Provider::DefaultSecretScope("aws") == vector<string> {""});
	try {
		S3Provider::MatchUrl("https://bucket/key");
		FAIL("Unsupported URL should fail");
	} catch (std::exception &ex) {
		auto error = string(ex.what());
		REQUIRE(error.find("s3a://") != string::npos);
		REQUIRE(error.find("s3n://") != string::npos);
		REQUIRE(error.find("gs://") != string::npos);
	}
}

TEST_CASE("S3 URL query settings are resolved independently of the HTTP client", "[httpfs][s3][url]") {
	SECTION("query credentials initialize the AWS region and endpoint") {
		S3AuthParams auth_params;
		auth_params.endpoint = "s3.amazonaws.com";
		auto parsed_url = S3Url::Resolve(
		    "s3://bucket/key?s3_access_key_id=hello+world&s3_secret_access_key=secret%2Bvalue", auth_params);
		REQUIRE(auth_params.access_key_id == "hello world");
		REQUIRE(auth_params.secret_access_key == "secret+value");
		REQUIRE(auth_params.region == "us-east-1");
		REQUIRE(auth_params.endpoint == "s3.us-east-1.amazonaws.com");
		REQUIRE(parsed_url.host == "bucket.s3.us-east-1.amazonaws.com");
	}

	SECTION("routing overrides are reflected in the parsed URL") {
		S3AuthParams auth_params;
		auth_params.region = "us-west-2";
		auth_params.endpoint = "s3.us-west-2.amazonaws.com";
		auto parsed_url = S3Url::Resolve("s3://bucket/key?s3_region=eu-west-1&s3_endpoint=s3.amazonaws.com&"
		                                 "s3_use_ssl=false&s3_url_style=path",
		                                 auth_params);
		REQUIRE(auth_params.region == "eu-west-1");
		REQUIRE(auth_params.endpoint == "s3.eu-west-1.amazonaws.com");
		REQUIRE(parsed_url.http_proto == "http://");
		REQUIRE(parsed_url.host == "s3.eu-west-1.amazonaws.com");
		REQUIRE(parsed_url.path == "/bucket/key");
	}

	SECTION("empty GCS routing overrides restore provider defaults") {
		S3AuthParams auth_params;
		auth_params.provider_type = S3ProviderType::GCS;
		auth_params.endpoint = "storage.googleapis.com";
		auth_params.url_style = "path";
		auto parsed_url = S3Url::Resolve("gcs://bucket/key?s3_endpoint=&s3_url_style=", auth_params);
		REQUIRE(auth_params.endpoint == "storage.googleapis.com");
		REQUIRE(auth_params.url_style == "path");
		REQUIRE(parsed_url.host == "storage.googleapis.com");
		REQUIRE(parsed_url.path == "/bucket/key");
	}

	SECTION("empty R2 endpoints fail instead of routing to AWS") {
		S3AuthParams auth_params;
		auth_params.provider_type = S3ProviderType::R2;
		auth_params.endpoint = "account.r2.cloudflarestorage.com";
		auth_params.url_style = "path";
		REQUIRE_THROWS(S3Url::Resolve("r2://bucket/key?s3_endpoint=", auth_params));
	}

	SECTION("empty values are accepted") {
		S3AuthParams auth_params;
		auth_params.endpoint = "s3.amazonaws.com";
		S3Url::Resolve("s3://bucket/key?s3_access_key_id&s3_secret_access_key=", auth_params);
		REQUIRE(auth_params.access_key_id.empty());
		REQUIRE(auth_params.secret_access_key.empty());
	}

	SECTION("duplicate decoded keys are rejected") {
		S3AuthParams auth_params;
		auth_params.endpoint = "s3.amazonaws.com";
		REQUIRE_THROWS(S3Url::Resolve("s3://bucket/key?s3_region=one&s3%5Fregion=two", auth_params));
	}

	SECTION("query keys remain case-sensitive") {
		S3AuthParams auth_params;
		auth_params.endpoint = "s3.amazonaws.com";
		REQUIRE_THROWS(S3Url::Resolve("s3://bucket/key?S3_region=one", auth_params));
	}

	SECTION("display URLs redact parameters unless compatibility mode treats them as key bytes") {
		S3AuthParams auth_params;
		REQUIRE(S3Url::GetDisplayUrl("s3://bucket/key?s3_secret_access_key=secret", auth_params) == "s3://bucket/key");
		auth_params.s3_url_compatibility_mode = true;
		REQUIRE(S3Url::GetDisplayUrl("s3://bucket/key?literal", auth_params) == "s3://bucket/key?literal");
	}
}

TEST_CASE("S3 URL compatibility mode reads canonical and legacy secret keys", "[httpfs][s3][secret]") {
	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::LoadExtension(db);

	SECTION("canonical key") {
		KeyValueSecret secret({"s3://"}, Identifier("s3"), Identifier("config"), Identifier("canonical"));
		secret.secret_map["url_compatibility_mode"] = Value(true);
		REQUIRE(ReadSecretAuthParams(con, secret).s3_url_compatibility_mode);
	}

	SECTION("canonical key takes precedence") {
		KeyValueSecret secret({"s3://"}, Identifier("s3"), Identifier("config"), Identifier("precedence"));
		secret.secret_map["url_compatibility_mode"] = Value(false);
		secret.secret_map["s3_url_compatibility_mode"] = Value(true);
		REQUIRE_FALSE(ReadSecretAuthParams(con, secret).s3_url_compatibility_mode);
	}

	SECTION("legacy key") {
		KeyValueSecret secret({"s3://"}, Identifier("s3"), Identifier("config"), Identifier("legacy"));
		secret.secret_map["s3_url_compatibility_mode"] = Value(true);
		REQUIRE(ReadSecretAuthParams(con, secret).s3_url_compatibility_mode);
	}

	SECTION("global setting fallback") {
		S3TestHelper::RequireQueryOk(con, "SET s3_url_compatibility_mode=true");
		KeyValueSecret secret({"s3://"}, Identifier("s3"), Identifier("config"), Identifier("setting"));
		REQUIRE(ReadSecretAuthParams(con, secret).s3_url_compatibility_mode);
	}
}

TEST_CASE("S3 provider policy selects request authentication", "[httpfs][s3][provider][signing]") {
	::AESStateSSLFactory encryption_util;
	ParsedS3Url parsed_url;
	parsed_url.path = "/bucket/key";
	parsed_url.host = "storage.example.com";

	SECTION("anonymous") {
		S3AuthParams auth_params;
		auto headers = S3RequestUtil::CreateHeaders(encryption_util, parsed_url, "", "GET", auth_params);
		REQUIRE(headers.GetHeaderValue("Host") == parsed_url.host);
		REQUIRE_FALSE(headers.HasHeader("Authorization"));
	}

	for (const auto provider_type : {S3ProviderType::S3, S3ProviderType::R2, S3ProviderType::GCS}) {
		S3AuthParams auth_params;
		auth_params.provider_type = provider_type;
		auth_params.region = "auto";
		auth_params.access_key_id = "key";
		auth_params.secret_access_key = "secret";
		auto headers = S3RequestUtil::CreateHeaders(encryption_util, parsed_url, "", "GET", auth_params, "20260806",
		                                            "20260806T120000Z");
		REQUIRE(StringUtil::StartsWith(headers.GetHeaderValue("Authorization"), "AWS4-HMAC-SHA256"));
	}

	SECTION("GCS bearer takes precedence over HMAC") {
		S3AuthParams auth_params;
		auth_params.provider_type = S3ProviderType::GCS;
		auth_params.access_key_id = "key";
		auth_params.secret_access_key = "secret";
		auth_params.oauth2_bearer_token = "token";
		auto headers = S3RequestUtil::CreateHeaders(encryption_util, parsed_url, "delete=", "POST", auth_params, "", "",
		                                            "payload", "application/xml", "content-md5");
		REQUIRE(headers.GetHeaderValue("Authorization") == "Bearer token");
		REQUIRE(headers.GetHeaderValue("Content-Type") == "application/xml");
		REQUIRE(headers.GetHeaderValue("Content-MD5") == "content-md5");
		REQUIRE_FALSE(headers.HasHeader("x-amz-date"));
	}
}

TEST_CASE("S3 request signing remains deterministic", "[httpfs][s3][signing]") {
	::AESStateSSLFactory encryption_util;
	S3AuthParams auth_params;
	auth_params.region = "us-east-1";
	auth_params.access_key_id = "AKIAIOSFODNN7EXAMPLE";
	auth_params.secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
	ParsedS3Url parsed_url;
	parsed_url.path = "/test.txt";
	parsed_url.host = "examplebucket.s3.amazonaws.com";

	auto headers = S3RequestUtil::CreateHeaders(encryption_util, parsed_url, "", "GET", auth_params, "20130524",
	                                            "20130524T000000Z");

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
	ParsedS3Url parsed_url;
	parsed_url.path = "/bucket/key";
	parsed_url.host = "bucket.example.com";

	auto headers = S3RequestUtil::CreateHeaders(encryption_util, parsed_url, "", "PUT", auth_params, "20260730",
	                                            "20260730T120000Z", "", "application/octet-stream", "content-md5");

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

TEST_CASE("GCS bearer authentication is shared by object, list and bulk-delete requests", "[httpfs][s3][gcs][bearer]") {
	SECTION("httplib") {
		RunGCSBearerRequestScenario("httplib");
	}
	SECTION("curl") {
		RunGCSBearerRequestScenario("curl");
	}
}

TEST_CASE("GCS list failures use provider-specific authentication diagnostics", "[httpfs][s3][gcs][errors]") {
	SECTION("httplib") {
		RunGCSListAuthErrorScenario("httplib");
	}
	SECTION("curl") {
		RunGCSListAuthErrorScenario("curl");
	}
}

} // namespace duckdb
