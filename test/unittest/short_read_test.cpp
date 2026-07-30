#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "httpfs.hpp"
#include "httpfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <atomic>
#include <thread>

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

static void ConfigureHTTPTest(DuckDB &db, Connection &con, idx_t retries, const string &client_implementation = "curl",
                              bool connection_caching = false) {
	LoadHTTPFSExtension(db);
	RequireQueryOk(con, "SET httpfs_client_implementation='" + client_implementation + "'");
	RequireQueryOk(con, StringUtil::Format("SET httpfs_connection_caching=%s", connection_caching ? "true" : "false"));
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

static vector<int> RequestPorts(const vector<MockS3RequestObservation> &observations, const string &method, int status,
                                const string &range = string()) {
	vector<int> result;
	for (auto &observation : observations) {
		if (observation.method == method && observation.status == status && observation.range == range) {
			result.push_back(observation.remote_port);
		}
	}
	return result;
}

static idx_t CountRangeRequests(const vector<MockS3RequestObservation> &observations, int status) {
	idx_t count = 0;
	for (auto &observation : observations) {
		if (observation.method == "GET" && observation.status == status && !observation.range.empty()) {
			count++;
		}
	}
	return count;
}

struct ReadOutcome {
	bool failed = false;
	bool internal_error = false;
	string error;
	string data;
};

static ReadOutcome TryReadHandle(Connection &con, FileHandle &handle, idx_t offset, idx_t length) {
	ReadOutcome outcome;
	try {
		string buffer(length, '?');
		handle.Read(QueryContext(*con.context), &buffer[0], length, offset);
		outcome.data = std::move(buffer);
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

static string ReadSequential(Connection &con, FileHandle &handle, idx_t length) {
	string buffer(length, '?');
	auto read_count = handle.Read(QueryContext(*con.context), buffer.data(), length);
	buffer.resize(NumericCast<idx_t>(read_count));
	return buffer;
}

static string CreateObjectData(idx_t length) {
	string result(length, '\0');
	for (idx_t i = 0; i < length; i++) {
		result[i] = NumericCast<char>('a' + (i % 26));
	}
	return result;
}

static void RunPersistentShortRead() {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	MockS3ServerConfig config;
	config.object.data = string(8192, 'x');
	config.range.behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range.behavior_requests = 4;
	config.range.truncated_bytes = 17;
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
	config.object.data = string(8192, 'x');
	config.range.behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range.behavior_requests = 1;
	config.range.truncated_bytes = 17;
	auto expected = config.object.data.substr(READ_OFFSET, READ_LENGTH);
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
	config.object.data = string(8192, 'x');
	config.range.behavior = MockS3RangeBehavior::SHORT_SUCCESS;
	config.range.behavior_requests = 1;
	config.range.truncated_bytes = 17;
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

static void RunParallelRangeHeaderRelease(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.data = string(8192, 'x');
	config.range.advertise = false;
	config.range.block_first_body_until_second = true;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0, client_implementation);
	RequireQueryOk(con, "BEGIN TRANSACTION");

	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto first_handle = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	auto second_handle = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);

	std::atomic<bool> start {false};
	ReadOutcome first_outcome;
	ReadOutcome second_outcome;
	std::thread first_reader([&]() {
		while (!start.load()) {
			std::this_thread::yield();
		}
		first_outcome = TryReadHandle(con, *first_handle, 0, 1024);
	});
	std::thread second_reader([&]() {
		while (!start.load()) {
			std::this_thread::yield();
		}
		second_outcome = TryReadHandle(con, *second_handle, 1024, 1024);
	});
	start = true;
	first_reader.join();
	second_reader.join();

	auto observations = server.Observations();
	INFO(first_outcome.error);
	INFO(second_outcome.error);
	INFO(MockS3DescribeObservations(observations));
	REQUIRE_FALSE(first_outcome.failed);
	REQUIRE_FALSE(second_outcome.failed);
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=0-1023") == 1);
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=1024-2047") == 1);
	RequireQueryOk(con, "COMMIT");
}

static void RunCompletedErrorConnectionReuse(const string &client_implementation) {
	MockS3ServerConfig config;
	config.failures.transient_head_failures = 1;
	config.failures.failure_is_request_timeout = false;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0, client_implementation);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(handle);
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	auto error_ports = RequestPorts(observations, "HEAD", 400);
	auto success_ports = RequestPorts(observations, "GET", 206, "bytes=0-1");
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
	ConfigureHTTPTest(db, con, 0, "curl", true);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	REQUIRE_FALSE(fs.FileExists(server.HTTPPath()));
	REQUIRE_FALSE(fs.FileExists(server.HTTPPath()));
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	auto not_found_ports = RequestPorts(observations, "HEAD", 404);
	REQUIRE(not_found_ports.size() == 2);
	REQUIRE(not_found_ports[0] != 0);
	REQUIRE(not_found_ports[0] == not_found_ports[1]);
}

static void RunFullDownloadSingleFlight(const string &client_implementation, bool shared_handle) {
	static constexpr idx_t READ_LENGTH = 1024;
	const vector<idx_t> offsets {0, 2048, 4096, 6144};

	MockS3ServerConfig config;
	config.object.data.resize(8192);
	for (idx_t i = 0; i < config.object.data.size(); i++) {
		config.object.data[i] = NumericCast<char>('a' + (i % 26));
	}
	const auto expected_data = config.object.data;
	config.range.advertise = false;
	config.range.behavior = MockS3RangeBehavior::IGNORE_RANGE;
	config.full_get.block_until_released = true;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0, client_implementation);
	RequireQueryOk(con, "BEGIN TRANSACTION");

	auto &fs = FileSystem::GetFileSystem(*con.context);
	const auto flags =
	    FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO | FileFlags::FILE_FLAGS_PARALLEL_ACCESS;
	vector<unique_ptr<FileHandle>> handles;
	const auto handle_count = shared_handle ? 1 : offsets.size();
	for (idx_t i = 0; i < handle_count; i++) {
		handles.push_back(fs.OpenFile(server.HTTPPath(), flags));
	}

	std::atomic<bool> start {false};
	vector<ReadOutcome> outcomes(offsets.size());
	vector<std::thread> readers;
	for (idx_t i = 0; i < offsets.size(); i++) {
		readers.emplace_back([&, i]() {
			while (!start.load()) {
				std::this_thread::yield();
			}
			auto &handle = shared_handle ? *handles[0] : *handles[i];
			outcomes[i] = TryReadHandle(con, handle, offsets[i], READ_LENGTH);
		});
	}
	start = true;
	const auto full_get_started = server.WaitForFullGet();
	server.ReleaseFullGet();
	for (auto &reader : readers) {
		reader.join();
	}

	REQUIRE(full_get_started);
	for (idx_t i = 0; i < outcomes.size(); i++) {
		INFO(outcomes[i].error);
		REQUIRE_FALSE(outcomes[i].failed);
		REQUIRE(outcomes[i].data == expected_data.substr(offsets[i], READ_LENGTH));
	}
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRangeRequests(observations, 200) == 1);
	REQUIRE(CountRequests(observations, "GET", 200) == 1);
	RequireQueryOk(con, "COMMIT");
}

static void RunExactSequentialReadGeometry(const string &client_implementation, FileOpenFlags flags) {
	MockS3ServerConfig config;
	config.object.data = CreateObjectData(8192);
	auto expected_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0, client_implementation);
	RequireQueryOk(con, "SET enable_external_file_cache=false");
	RequireQueryOk(con, "BEGIN TRANSACTION");

	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(server.HTTPPath(), flags);

	REQUIRE(ReadSequential(con, *handle, 13) == expected_data.substr(0, 13));
	REQUIRE(ReadSequential(con, *handle, 17) == expected_data.substr(13, 17));
	REQUIRE(handle->SeekPosition() == 30);

	handle->Seek(1024);
	REQUIRE(ReadSequential(con, *handle, 19) == expected_data.substr(1024, 19));
	REQUIRE(handle->SeekPosition() == 1043);

	auto observations_before_empty_reads = server.Observations();
	REQUIRE(ReadSequential(con, *handle, 0).empty());
	handle->Seek(expected_data.size());
	REQUIRE(ReadSequential(con, *handle, 1).empty());
	REQUIRE(server.Observations().size() == observations_before_empty_reads.size());

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=0-12") == 1);
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=13-29") == 1);
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=1024-1042") == 1);
	REQUIRE(CountRangeRequests(observations, 206) == 3);
	RequireQueryOk(con, "COMMIT");
}

static void RunSequentialFailureKeepsPosition(const string &client_implementation) {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;

	MockS3ServerConfig config;
	config.object.data = CreateObjectData(8192);
	auto expected_data = config.object.data;
	config.range.behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range.behavior_requests = 1;
	config.range.truncated_bytes = 17;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0, client_implementation);
	RequireQueryOk(con, "SET enable_external_file_cache=false");
	RequireQueryOk(con, "BEGIN TRANSACTION");

	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ);
	handle->Seek(READ_OFFSET);

	ReadOutcome outcome;
	try {
		ReadSequential(con, *handle, READ_LENGTH);
	} catch (std::exception &ex) {
		outcome.failed = true;
		outcome.error = ex.what();
		ErrorData error(ex);
		outcome.internal_error = error.Type() == ExceptionType::INTERNAL || error.Type() == ExceptionType::FATAL;
	}
	INFO(outcome.error);
	REQUIRE(outcome.failed);
	REQUIRE_FALSE(outcome.internal_error);
	REQUIRE(handle->SeekPosition() == READ_OFFSET);

	REQUIRE(ReadSequential(con, *handle, 31) == expected_data.substr(READ_OFFSET, 31));
	REQUIRE(handle->SeekPosition() == READ_OFFSET + 31);

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=113-1136") == 1);
	REQUIRE(CountRequests(observations, "GET", 206, "bytes=113-143") == 1);
	RequireQueryOk(con, "COMMIT");
}

static void RunSequentialFullDownloadFallback(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.data = CreateObjectData(8192);
	auto expected_data = config.object.data;
	config.range.advertise = false;
	config.range.behavior = MockS3RangeBehavior::IGNORE_RANGE;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureHTTPTest(db, con, 0, client_implementation);
	RequireQueryOk(con, "SET enable_external_file_cache=false");
	RequireQueryOk(con, "BEGIN TRANSACTION");

	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ);
	REQUIRE(ReadSequential(con, *handle, 17) == expected_data.substr(0, 17));
	REQUIRE(handle->SeekPosition() == 17);
	REQUIRE(ReadSequential(con, *handle, 11) == expected_data.substr(17, 11));
	REQUIRE(handle->SeekPosition() == 28);

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "GET", 200, "bytes=0-16") == 1);
	REQUIRE(CountRequests(observations, "GET", 200) == 1);
	RequireQueryOk(con, "COMMIT");
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

TEST_CASE("HTTP range readers resume after the first response headers", "[httpfs][range-probe]") {
	SECTION("curl") {
		RunParallelRangeHeaderRelease("curl");
	}
	SECTION("httplib") {
		RunParallelRangeHeaderRelease("httplib");
	}
}

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

TEST_CASE("HTTP full-download fallback is single-flight", "[httpfs][full-download][range-probe]") {
	SECTION("curl with separate handles") {
		RunFullDownloadSingleFlight("curl", false);
	}
	SECTION("curl with a shared handle") {
		RunFullDownloadSingleFlight("curl", true);
	}
	SECTION("httplib with separate handles") {
		RunFullDownloadSingleFlight("httplib", false);
	}
	SECTION("httplib with a shared handle") {
		RunFullDownloadSingleFlight("httplib", true);
	}
}

TEST_CASE("HTTP full-download fallback rejects HEAD and GET length mismatches", "[httpfs][full-download][issue-354]") {
	MockS3ServerConfig config;
	config.object.data = "AB";
	config.metadata.head_content_length = 3;
	config.range.behavior = MockS3RangeBehavior::IGNORE_RANGE;
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

TEST_CASE("HTTP sequential reads issue exact ranges without read-ahead", "[httpfs][read-ahead]") {
	SECTION("curl default flags") {
		RunExactSequentialReadGeometry("curl", FileFlags::FILE_FLAGS_READ);
	}
	SECTION("curl DirectIO") {
		RunExactSequentialReadGeometry("curl", FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	}
	SECTION("curl parallel access") {
		RunExactSequentialReadGeometry("curl", FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_PARALLEL_ACCESS);
	}
	SECTION("httplib default flags") {
		RunExactSequentialReadGeometry("httplib", FileFlags::FILE_FLAGS_READ);
	}
	SECTION("httplib DirectIO") {
		RunExactSequentialReadGeometry("httplib", FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	}
	SECTION("httplib parallel access") {
		RunExactSequentialReadGeometry("httplib", FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_PARALLEL_ACCESS);
	}
}

TEST_CASE("HTTP sequential read failures leave the cursor unchanged", "[httpfs][read-ahead][short-read]") {
	SECTION("curl") {
		RunSequentialFailureKeepsPosition("curl");
	}
	SECTION("httplib") {
		RunSequentialFailureKeepsPosition("httplib");
	}
}

TEST_CASE("HTTP sequential reads retain full-download fallback", "[httpfs][read-ahead][full-download]") {
	SECTION("curl") {
		RunSequentialFullDownloadFallback("curl");
	}
	SECTION("httplib") {
		RunSequentialFullDownloadFallback("httplib");
	}
}

} // namespace duckdb
