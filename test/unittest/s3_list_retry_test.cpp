#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "httpfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

static void LoadHTTPFSExtension(DuckDB &db) {
	if (db.ExtensionIsLoaded("httpfs")) {
		return;
	}
	ExtensionInfo extension_info;
	ExtensionActiveLoad load_info(*db.instance, extension_info, "httpfs");
	ExtensionLoader loader(load_info);
	HttpfsExtension extension;
	extension.Load(loader);
}

static void RequireQueryOk(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE(result);
	INFO((result->HasError() ? result->GetError() : string()));
	REQUIRE_FALSE(result->HasError());
}

static void ConfigureListRetryTest(DuckDB &db, Connection &con, MockS3Server &server,
                                   const string &client_implementation, idx_t retries) {
	LoadHTTPFSExtension(db);

	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, "SET http_retries=" + to_string(retries));
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "SET http_retry_backoff=2");
	RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET list_retry_s3 (
	TYPE S3,
	PROVIDER CONFIG,
	SCOPE 's3://refresh-bucket/',
	KEY_ID 'FRESH_KEY',
	SECRET 'FRESH_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path'
))",
	                                       server.Endpoint()));
}

static idx_t CountListObservations(const vector<MockS3RequestObservation> &observations, int status) {
	idx_t result = 0;
	for (auto &observation : observations) {
		if (observation.method == "GET" && observation.target.find("list-type=2") != string::npos &&
		    observation.status == status) {
			result++;
		}
	}
	return result;
}

static void RunRecoveringListRetryTest(const string &client_implementation, int status) {
	MockS3ServerConfig config;
	if (status == 503) {
		config.transient_503_lists = 1;
	} else {
		D_ASSERT(status == 400);
		config.transient_400_lists = 1;
	}
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureListRetryTest(db, con, server, client_implementation, 2);

	auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/*.bin') ORDER BY file");
	REQUIRE(result);
	auto observations = server.Observations();
	INFO((result->HasError() ? result->GetError() : string()));
	INFO(MockS3DescribeObservations(observations));
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0).ToString() == "s3://refresh-bucket/object.bin");

	REQUIRE(CountListObservations(observations, status) == 1);
	REQUIRE(CountListObservations(observations, 200) >= 1);
}

static void RunExhaustedListRetryTest(const string &client_implementation, int status) {
	MockS3ServerConfig config;
	if (status == 503) {
		config.transient_503_lists = 1000;
	} else {
		D_ASSERT(status == 400);
		config.transient_400_lists = 1000;
	}
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureListRetryTest(db, con, server, client_implementation, 2);

	auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/*.bin')");
	REQUIRE(result);
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), to_string(status)));

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// One initial attempt plus two retries.
	REQUIRE(CountListObservations(observations, status) == 3);
	REQUIRE(CountListObservations(observations, 200) == 0);
}

static void RunGeneric400ListTest(const string &client_implementation) {
	MockS3ServerConfig config;
	config.transient_400_lists = 1000;
	config.failure_is_request_timeout = false;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureListRetryTest(db, con, server, client_implementation, 2);

	auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/*.bin')");
	REQUIRE(result);
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "InvalidRequest"));

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountListObservations(observations, 400) == 1);
	REQUIRE(CountListObservations(observations, 200) == 0);
}

} // namespace

TEST_CASE("S3 glob recovers from transient ListObjectsV2 errors", "[httpfs][s3][retry]") {
	SECTION("httplib retries 503") {
		RunRecoveringListRetryTest("httplib", 503);
	}
	SECTION("curl retries 503") {
		RunRecoveringListRetryTest("curl", 503);
	}
	SECTION("httplib retries 400 RequestTimeout") {
		RunRecoveringListRetryTest("httplib", 400);
	}
	SECTION("curl retries 400 RequestTimeout") {
		RunRecoveringListRetryTest("curl", 400);
	}
}

TEST_CASE("S3 glob exhausts retries for persistent transient ListObjectsV2 errors", "[httpfs][s3][retry]") {
	SECTION("httplib exhausts 503 retries") {
		RunExhaustedListRetryTest("httplib", 503);
	}
	SECTION("curl exhausts 503 retries") {
		RunExhaustedListRetryTest("curl", 503);
	}
	SECTION("httplib exhausts 400 RequestTimeout retries") {
		RunExhaustedListRetryTest("httplib", 400);
	}
	SECTION("curl exhausts 400 RequestTimeout retries") {
		RunExhaustedListRetryTest("curl", 400);
	}
}

TEST_CASE("S3 glob does not retry a generic ListObjectsV2 400", "[httpfs][s3][retry]") {
	SECTION("httplib") {
		RunGeneric400ListTest("httplib");
	}
	SECTION("curl") {
		RunGeneric400ListTest("curl");
	}
}

} // namespace duckdb
