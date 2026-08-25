#include "catch.hpp"

#include "duckdb.hpp"

namespace duckdb {

namespace {

static bool IsNotRouted(const string &error) {
	// No filesystem accepted the path
	return error.find("No such file or directory") != string::npos || error.find("No files found") != string::npos;
}

static bool IsRoutedToS3(const string &error) {
	// Errors only the S3 filesystem produces, so reaching either means it handled the path
	return error.find("URL needs to contain a bucket name") != string::npos ||
	       error.find("aliased URL scheme requires an endpoint") != string::npos;
}

static string RouteError(Connection &con) {
	auto result = con.Query("FROM 'oss:///test.csv'");
	REQUIRE(result->HasError());
	return result->GetError();
}

static string AliasSetting(Connection &con) {
	auto result = con.Query("SELECT current_setting('s3_url_scheme_aliases')");
	REQUIRE_FALSE(result->HasError());
	return result->GetValue(0, 0).ToString();
}

} // namespace

TEST_CASE("s3_url_scheme_aliases is isolated per database instance", "[httpfs][s3]") {
	DuckDB db_a(nullptr);
	DuckDB db_b(nullptr);
	Connection con_a(db_a);
	Connection con_b(db_b);

	REQUIRE(IsNotRouted(RouteError(con_a)));
	REQUIRE(IsNotRouted(RouteError(con_b)));

	// Registering the alias in database A must not affect database B
	REQUIRE_FALSE(con_a.Query("SET s3_url_scheme_aliases = ['oss']")->HasError());
	REQUIRE(IsRoutedToS3(RouteError(con_a)));
	REQUIRE(IsNotRouted(RouteError(con_b)));
	REQUIRE(AliasSetting(con_a) == "[oss]");
	REQUIRE(AliasSetting(con_b) == "[]");

	// Resetting in database B must not affect database A
	REQUIRE_FALSE(con_b.Query("RESET s3_url_scheme_aliases")->HasError());
	REQUIRE(IsRoutedToS3(RouteError(con_a)));
	REQUIRE(IsNotRouted(RouteError(con_b)));
}

TEST_CASE("s3_url_scheme_aliases stored without the setting callback cannot hijack a scheme", "[httpfs][s3]") {
	DuckDB db(nullptr);
	Connection con(db);

	// DBConfig::SetOptionByName stores extension settings without running their callback, so routing
	// must not trust the stored value
	vector<Value> aliases {Value("https"), Value("file"), Value("not a scheme")};
	DBConfig::GetConfig(*db.instance)
	    .SetOptionByName("s3_url_scheme_aliases", Value::LIST(LogicalType::VARCHAR, aliases));

	auto https_result = con.Query("FROM 'https:///test.csv'");
	REQUIRE(https_result->HasError());
	REQUIRE(https_result->GetError().find("URL needs to contain a bucket name") == string::npos);

	REQUIRE(IsNotRouted(RouteError(con)));
}

} // namespace duckdb
