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

static void ConfigureListRetryTest(DuckDB &db, Connection &con, MockS3Server &server) {
	LoadHTTPFSExtension(db);

	RequireQueryOk(con, "SET http_retries=5");
	RequireQueryOk(con, "SET http_retry_wait_ms=50");
	RequireQueryOk(con, "SET http_retry_backoff=2");
	RequireQueryOk(con, StringUtil::Format("SET s3_endpoint='%s'", server.Endpoint()));
	RequireQueryOk(con, "SET s3_region='us-east-1'");
	RequireQueryOk(con, "SET s3_use_ssl=false");
	RequireQueryOk(con, "SET s3_url_style='path'");
	RequireQueryOk(con, "SET s3_access_key_id='FRESH_KEY'");
	RequireQueryOk(con, "SET s3_secret_access_key='FRESH_SECRET'");
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

} // namespace

TEST_CASE("S3 glob recovers from a transient 503 on ListObjectsV2", "[httpfs][s3][retry]") {
	MockS3ServerConfig config;
	config.transient_503_lists = 1;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureListRetryTest(db, con, server);

	// The first LIST returns 503 SlowDown; the in-request retry must recover
	// instead of throwing the first attempt's error after the fact
	auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/*.bin') ORDER BY file");
	REQUIRE(result);
	INFO((result->HasError() ? result->GetError() : string()));
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->RowCount() == 1);
	// The recovered listing parses cleanly: the SlowDown error body from the
	// failed attempt must not be prepended to the response
	REQUIRE(result->GetValue(0, 0).ToString() == "s3://refresh-bucket/object.bin");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountListObservations(observations, 503) == 1);
	REQUIRE(CountListObservations(observations, 200) >= 1);
}

TEST_CASE("S3 glob still fails when 503s outlast the configured retries", "[httpfs][s3][retry]") {
	MockS3ServerConfig config;
	config.transient_503_lists = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureListRetryTest(db, con, server);

	auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/*.bin')");
	REQUIRE(result);
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "503"));

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// initial attempt + http_retries, all 503
	REQUIRE(CountListObservations(observations, 503) == 6);
	REQUIRE(CountListObservations(observations, 200) == 0);
}

} // namespace duckdb
