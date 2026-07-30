#include "catch.hpp"

#include "http/http_test_helper.hpp"

namespace duckdb {

namespace {

static void RunCompletedErrorConnectionReuse(const string &client_implementation) {
	MockS3ServerConfig config;
	config.failures.transient_head_failures = 1;
	config.failures.failure_is_request_timeout = false;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	HTTPTestHelper::Configure(db, con, 0, client_implementation);

	HTTPTestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(handle);
	HTTPTestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	auto error_ports = HTTPTestHelper::RequestPorts(observations, "HEAD", 400);
	auto success_ports = HTTPTestHelper::RequestPorts(observations, "GET", 206, "bytes=0-1");
	REQUIRE(error_ports.size() == 1);
	REQUIRE(success_ports.size() == 1);
	REQUIRE(error_ports[0] != 0);
	REQUIRE(error_ports[0] == success_ports[0]);
}

static void RunSharedConnectionNotFoundReuse() {
	MockS3ServerConfig config;
	config.failures.head_not_found_requests = 2;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	HTTPTestHelper::Configure(db, con, 0, "curl", true);

	HTTPTestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	REQUIRE_FALSE(fs.FileExists(server.HTTPPath()));
	REQUIRE_FALSE(fs.FileExists(server.HTTPPath()));
	HTTPTestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	auto not_found_ports = HTTPTestHelper::RequestPorts(observations, "HEAD", 404);
	REQUIRE(not_found_ports.size() == 2);
	REQUIRE(not_found_ports[0] != 0);
	REQUIRE(not_found_ports[0] == not_found_ports[1]);
}

} // namespace

TEST_CASE("HTTP request sessions reuse connections after completed errors", "[httpfs][request-session]") {
	SECTION("httplib reuses a session-local connection") {
		RunCompletedErrorConnectionReuse("httplib");
	}
	SECTION("curl reuses a session-local connection") {
		RunCompletedErrorConnectionReuse("curl");
	}
	SECTION("curl reuses a shared connection across missing-file probes") {
		RunSharedConnectionNotFoundReuse();
	}
}

} // namespace duckdb
