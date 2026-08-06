#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "s3/s3fs.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

static void RunTransientPutRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	// Use a refresh target that writes never exercise, so credential refresh never triggers here.
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 2;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	S3TestHelper::WriteMultipartPayload(con);
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// The transient RequestTimeout 400s were surfaced, retried, and the same part eventually succeeded.
	REQUIRE(MockS3HasObservation(observations, "PUT", S3TestHelper::STALE_KEY_ID, 400, string(), "partNumber"));
	REQUIRE(MockS3HasObservation(observations, "PUT", S3TestHelper::STALE_KEY_ID, 200, string(), "partNumber"));
	// The multipart upload completed successfully.
	REQUIRE(MockS3HasObservation(observations, "POST", S3TestHelper::STALE_KEY_ID, 200, string(), "uploadId"));
}

static idx_t CountObservationsTarget(const vector<MockS3RequestObservation> &observations, const string &method,
                                     int status, const string &target_contains) {
	idx_t result = 0;
	for (auto &observation : observations) {
		if (observation.method == method && observation.status == status &&
		    (target_contains.empty() || observation.target.find(target_contains) != string::npos)) {
			result++;
		}
	}
	return result;
}

// A generic (non-RequestTimeout) 400 must fail on the first try. Uses a single-shot PUT so exactly one
// request is expected, distinguishing "not retried" from "retried N times and still failed".
static void RunGenericErrorNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1000;
	config.failures.failure_is_request_timeout = false;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(S3TestHelper::WriteSinglePutPayload(con));
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(S3TestHelper::CountObservations(observations, "PUT", S3TestHelper::STALE_KEY_ID, 400) == 1);
}

// A truncated error body (an open <Code> with no closing tag) must degrade to a plain HTTP error:
// not classified as transient (no retry) and never escalated to a DB-invalidating InternalException.
static void RunTruncatedErrorBodyScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1000;
	config.failures.truncated_failure_body = true;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	bool threw = false;
	bool threw_internal = false;
	try {
		S3TestHelper::WriteSinglePutPayload(con);
	} catch (std::exception &ex) {
		threw = true;
		ErrorData error(ex);
		threw_internal = error.Type() == ExceptionType::INTERNAL || error.Type() == ExceptionType::FATAL;
	}
	REQUIRE(threw);
	REQUIRE_FALSE(threw_internal);
	// The database must still be usable (an InternalException would have invalidated it).
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(S3TestHelper::CountObservations(observations, "PUT", S3TestHelper::STALE_KEY_ID, 400) == 1);
}

// Even a retryable RequestTimeout must NOT replay a multipart-init POST (it would orphan an upload id).
static void RunMultipartInitNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_post_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(S3TestHelper::WriteMultipartPayload(con));
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservationsTarget(observations, "POST", 400, "uploads") == 1);
}

// A RequestTimeout retry must run on a fresh connection instead of the stalled one.
static void RunFreshConnectionRetryScenario(const string &client_implementation, bool connection_caching) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, connection_caching);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	S3TestHelper::WriteSinglePutPayload(con);
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	int failed_port = 0;
	int success_port = 0;
	for (auto &observation : observations) {
		if (observation.method != "PUT") {
			continue;
		}
		if (observation.status == 400) {
			failed_port = observation.remote_port;
		} else if (observation.status == 200) {
			success_port = observation.remote_port;
		}
	}
	REQUIRE(failed_port != 0);
	REQUIRE(success_port != 0);
	REQUIRE(failed_port != success_port);
}

// A RequestTimeout that never clears exhausts exactly http_retries retries (one initial + http_retries).
static void RunRetryBudgetScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=2");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(S3TestHelper::WriteSinglePutPayload(con));
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// One initial attempt plus http_retries (2) retries.
	REQUIRE(S3TestHelper::CountObservations(observations, "PUT", S3TestHelper::STALE_KEY_ID, 400) == 3);
}

static void RunTransientGetRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_get_failures = 2;
	auto object_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "SET force_download=true");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ);
	string buffer(8, '\0');
	handle->Read(QueryContext(*con.context), &buffer[0], buffer.size(), 0);
	REQUIRE(buffer == object_data.substr(0, buffer.size()));
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// The read hit transient RequestTimeout 400s and was retried until it succeeded.
	REQUIRE(MockS3HasObservation(observations, "GET", S3TestHelper::STALE_KEY_ID, 400));
	REQUIRE(MockS3HasObservation(observations, "GET", S3TestHelper::STALE_KEY_ID, 200));
}

static void RunTransientDeleteRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	// Use a refresh target that deletes never exercise, so credential refresh never triggers here.
	config.auth.refresh_target = MockS3RefreshTarget::PUT;
	config.failures.transient_delete_failures = 2;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	fs.RemoveFile(S3TestHelper::S3_PATH);
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(S3TestHelper::CountObservations(observations, "DELETE", S3TestHelper::STALE_KEY_ID, 400) == 2);
	REQUIRE(S3TestHelper::CountObservations(observations, "DELETE", S3TestHelper::STALE_KEY_ID, 204) == 1);
}

// HEAD responses carry no body, so a RequestTimeout 400 cannot be classified as transient:
// the HEAD is not retried and the open recovers through httpfs's range-GET fallback instead.
static void RunHeadNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_head_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(handle);
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(S3TestHelper::CountObservations(observations, "HEAD", S3TestHelper::STALE_KEY_ID, 400) == 1);
	REQUIRE(MockS3HasObservation(observations, "GET", S3TestHelper::STALE_KEY_ID, 206, "bytes=0-1"));
	auto error_ports = S3TestHelper::ObservationPorts(observations, "HEAD", S3TestHelper::STALE_KEY_ID, 400);
	auto success_ports =
	    S3TestHelper::ObservationPorts(observations, "GET", S3TestHelper::STALE_KEY_ID, 206, "bytes=0-1");
	REQUIRE(error_ports.size() == 1);
	REQUIRE(success_ports.size() == 1);
	REQUIRE(error_ports[0] != 0);
	REQUIRE(error_ports[0] == success_ports[0]);
}

// Like multipart-init, a multipart-complete POST must not be replayed even on RequestTimeout.
static void RunMultipartCompleteNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_complete_post_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(S3TestHelper::WriteMultipartPayload(con));
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservationsTarget(observations, "POST", 400, "uploadId") == 1);
}

static void RunCompletePost200ErrorRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_complete_post_200_errors = 2;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	S3TestHelper::WriteMultipartPayload(con);
	S3TestHelper::RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservationsTarget(observations, "POST", 200, "uploadId") == 3);
	REQUIRE(CountObservationsTarget(observations, "DELETE", 204, "uploadId") == 0);
	REQUIRE(server.UploadedObject().size() == 10 * 1024 * 1024 + 1);
}

static void RunCompletePost200ErrorExhaustsBudgetScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = S3TestHelper::BUCKET;
	config.object.key = S3TestHelper::OBJECT_KEY;
	config.auth.stale_key_id = S3TestHelper::STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_complete_post_200_errors = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);

	S3TestHelper::RequireQueryOk(con, "SET http_retries=3");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
	ErrorData error;
	bool threw = false;
	try {
		S3TestHelper::WriteMultipartPayload(con);
	} catch (std::exception &ex) {
		threw = true;
		error = ErrorData(ex);
	}
	S3TestHelper::RequireQueryOk(con, "ROLLBACK");

	REQUIRE(threw);
	REQUIRE(error.Type() == ExceptionType::HTTP);
	REQUIRE(error.ExtraInfo().at("status_code") == "200");
	REQUIRE(StringUtil::Contains(error.ExtraInfo().at("response_body"), "InternalError"));
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservationsTarget(observations, "POST", 200, "uploadId") == 4);
	REQUIRE(CountObservationsTarget(observations, "DELETE", 204, "uploadId") == 1);
}

static void RunAllTransientRetryScenarios(const string &client_implementation) {
	SECTION("multipart part upload retries and completes") {
		RunTransientPutRetryScenario(client_implementation);
	}
	SECTION("object read retries and completes") {
		RunTransientGetRetryScenario(client_implementation);
	}
	SECTION("object delete retries and completes") {
		RunTransientDeleteRetryScenario(client_implementation);
	}
	SECTION("a generic 400 is not retried") {
		RunGenericErrorNotRetriedScenario(client_implementation);
	}
	SECTION("a truncated error body is not retried and does not invalidate the database") {
		RunTruncatedErrorBodyScenario(client_implementation);
	}
	SECTION("a multipart-init POST is not retried even on RequestTimeout") {
		RunMultipartInitNotRetriedScenario(client_implementation);
	}
	SECTION("a multipart-complete POST is not retried even on RequestTimeout") {
		RunMultipartCompleteNotRetriedScenario(client_implementation);
	}
	SECTION("a multipart-complete 200 InternalError is retried") {
		RunCompletePost200ErrorRetryScenario(client_implementation);
	}
	SECTION("multipart-complete 200 InternalError retries are bounded by http_retries") {
		RunCompletePost200ErrorExhaustsBudgetScenario(client_implementation);
	}
	SECTION("a HEAD 400 is not retried because HEAD responses carry no error body") {
		RunHeadNotRetriedScenario(client_implementation);
	}
	SECTION("retries are bounded by http_retries") {
		RunRetryBudgetScenario(client_implementation);
	}
}

static void ConfigureListRetryTest(DuckDB &db, Connection &con, MockS3Server &server,
                                   const string &client_implementation, idx_t retries) {
	S3TestHelper::LoadExtension(db);

	S3TestHelper::RequireQueryOk(con,
	                             StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	S3TestHelper::RequireQueryOk(con, "SET http_retries=" + to_string(retries));
	S3TestHelper::RequireQueryOk(con, "SET http_retry_wait_ms=1");
	S3TestHelper::RequireQueryOk(con, "SET http_retry_backoff=2");
	S3TestHelper::RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET list_retry_s3 (
	TYPE S3,
	PROVIDER CONFIG,
	SCOPE 's3://refresh-bucket/',
	KEY_ID 'FRESH_KEY',
	SECRET 'S3TestHelper::FRESH_SECRET',
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
		config.failures.transient_503_lists = 1;
	} else {
		D_ASSERT(status == 400);
		config.failures.transient_400_lists = 1;
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
		config.failures.transient_503_lists = 1000;
	} else {
		D_ASSERT(status == 400);
		config.failures.transient_400_lists = 1000;
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
	// Core grants five extra retries to 503 throttling responses.
	const idx_t expected_attempts = status == 503 ? 8 : 3;
	REQUIRE(CountListObservations(observations, status) == expected_attempts);
	REQUIRE(CountListObservations(observations, 200) == 0);
}

static void RunGeneric400ListTest(const string &client_implementation) {
	MockS3ServerConfig config;
	config.failures.transient_400_lists = 1000;
	config.failures.failure_is_request_timeout = false;
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

TEST_CASE("HTTPFS retries transient S3 RequestTimeout across request types", "[httpfs][s3][upload]") {
	SECTION("httplib") {
		RunAllTransientRetryScenarios("httplib");
	}
	SECTION("curl") {
		RunAllTransientRetryScenarios("curl");
	}
	SECTION("httplib retries RequestTimeout on a fresh connection") {
		RunFreshConnectionRetryScenario("httplib", false);
	}
	SECTION("session-local curl retries RequestTimeout on a fresh connection") {
		RunFreshConnectionRetryScenario("curl", false);
	}
	SECTION("shared curl retries RequestTimeout on a fresh connection") {
		RunFreshConnectionRetryScenario("curl", true);
	}
}

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
