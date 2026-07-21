#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "httpfs.hpp"
#include "httpfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

static void LoadHTTPFSExtension(DuckDB &db) {
	if (db.ExtensionIsLoaded("httpfs")) {
		return;
	}
	ExtensionInfo extension_info;
	ExtensionActiveLoad load_info(*db.instance, extension_info, "httpfs", "");
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

static void ConfigureHTTPTest(DuckDB &db, Connection &con, idx_t retries) {
	LoadHTTPFSExtension(db);
	RequireQueryOk(con, "SET httpfs_client_implementation='curl'");
	RequireQueryOk(con, "SET httpfs_connection_caching=false");
	RequireQueryOk(con, "SET http_retries=" + to_string(retries));
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "SET http_retry_backoff=1");
}

static idx_t CountRequests(const vector<MockS3RequestObservation> &observations, const string &method, int status,
                           const string &range = string()) {
	idx_t count = 0;
	for (auto &observation : observations) {
		if (observation.method == method && observation.status == status && observation.range == range) {
			count++;
		}
	}
	return count;
}

struct ReadOutcome {
	bool failed = false;
	bool internal_error = false;
	string error;
};

static ReadOutcome TryReadHandle(Connection &con, FileHandle &handle, idx_t offset, idx_t length) {
	ReadOutcome outcome;
	try {
		string buffer(length, '?');
		handle.Read(QueryContext(*con.context), &buffer[0], length, offset);
	} catch (std::exception &ex) {
		outcome.failed = true;
		outcome.error = ex.what();
		ErrorData error(ex);
		outcome.internal_error = error.Type() == ExceptionType::INTERNAL || error.Type() == ExceptionType::FATAL;
	} catch (...) {
		outcome.failed = true;
		outcome.error = "unknown exception";
	}
	return outcome;
}

static ReadOutcome TryReadRange(Connection &con, const string &path, idx_t offset, idx_t length) {
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	return TryReadHandle(con, *handle, offset, length);
}

static string ReadRange(Connection &con, const string &path, idx_t offset, idx_t length) {
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	string buffer(length, '?');
	handle->Read(QueryContext(*con.context), &buffer[0], length, offset);
	return buffer;
}

static void RunPersistentShortRead() {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	MockS3ServerConfig config;
	config.object_data = string(8192, 'x');
	config.range_behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range_behavior_requests = 4;
	config.truncated_range_bytes = 17;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 3);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto outcome = TryReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
	RequireQueryOk(con, "ROLLBACK");
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	INFO(outcome.error);
	REQUIRE(outcome.failed);
	REQUIRE_FALSE(outcome.internal_error);
	REQUIRE(CountRequests(observations, "GET", 206, expected_range) == 4);
}

static void RunTransientShortRead() {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	MockS3ServerConfig config;
	config.object_data = string(8192, 'x');
	config.range_behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range_behavior_requests = 1;
	config.truncated_range_bytes = 17;
	auto expected = config.object_data.substr(READ_OFFSET, READ_LENGTH);
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 1);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto actual = ReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
	RequireQueryOk(con, "COMMIT");
	REQUIRE(actual == expected);
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "GET", 206, expected_range) == 2);
}

static void RunSuccessfulShortResponse() {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	MockS3ServerConfig config;
	config.object_data = string(8192, 'x');
	config.range_behavior = MockS3RangeBehavior::SHORT_SUCCESS;
	config.range_behavior_requests = 1;
	config.truncated_range_bytes = 17;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto outcome = TryReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
	RequireQueryOk(con, "ROLLBACK");
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	INFO(outcome.error);
	REQUIRE(outcome.failed);
	REQUIRE_FALSE(outcome.internal_error);
	REQUIRE(outcome.error.find("Short read for HTTP GET") != string::npos);
	REQUIRE(CountRequests(observations, "GET", 206, expected_range) == 1);
}

} // namespace

TEST_CASE("HTTP range reads reject truncated response bodies", "[httpfs][short-read]") {
	SECTION("persistent transfer truncation exhausts retries") {
		RunPersistentShortRead();
	}
	SECTION("transient transfer truncation is replaced by a complete retry") {
		RunTransientShortRead();
	}
	SECTION("a cleanly terminated short response is rejected") {
		RunSuccessfulShortResponse();
	}
}

TEST_CASE("HTTP full-download fallback rejects HEAD and GET length mismatches", "[httpfs][full-download][issue-354]") {
	MockS3ServerConfig config;
	config.object_data = "AB";
	config.head_content_length = 3;
	config.range_behavior = MockS3RangeBehavior::IGNORE_RANGE;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0);
	RequireQueryOk(con, "SET enable_http_metadata_cache=true");

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto initial_outcome = TryReadRange(con, server.HTTPPath(), 0, 1);
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	INFO(initial_outcome.error);
	REQUIRE(initial_outcome.failed);
	REQUIRE_FALSE(initial_outcome.internal_error);
	REQUIRE(initial_outcome.error.find("size reported by HEAD") != string::npos);
	REQUIRE(initial_outcome.error.find("full GET downloaded") != string::npos);
	REQUIRE(CountRequests(observations, "HEAD", 200) == 1);
	REQUIRE(CountRequests(observations, "GET", 200, "bytes=0-0") == 1);
	REQUIRE(CountRequests(observations, "GET", 200) == 1);

	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto reopened = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(reopened->Cast<HTTPFileHandle>().etag == "\"httpfs-refresh-test-etag\"");
	auto reopened_outcome = TryReadHandle(con, *reopened, 0, 1);
	INFO(reopened_outcome.error);
	REQUIRE(reopened_outcome.failed);
	REQUIRE_FALSE(reopened_outcome.internal_error);

	observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "HEAD", 200) == 1);
	REQUIRE(CountRequests(observations, "GET", 200, "bytes=0-0") == 2);
	REQUIRE(CountRequests(observations, "GET", 200) == 2);
	RequireQueryOk(con, "SELECT 42");

	RequireQueryOk(con, "SET force_download=true");
	REQUIRE(ReadRange(con, server.HTTPPath(), 0, 2) == "AB");
	RequireQueryOk(con, "COMMIT");
}

} // namespace duckdb
