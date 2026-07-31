#include "catch.hpp"

#include "s3/mock_s3_server.hpp"
#include "s3/s3_test_helper.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/async_file_writer.hpp"
#include "duckdb/common/string_util.hpp"

#include <future>
#include <thread>

namespace duckdb {

namespace {

struct S3UploadBaselineTest {
	static constexpr idx_t MINIMUM_PART_SIZE = 5ULL * 1024ULL * 1024ULL;

	template <class CALLBACK>
	static string RequireError(CALLBACK callback) {
		try {
			callback();
		} catch (std::exception &ex) {
			return ex.what();
		}
		FAIL("Expected operation to throw");
		return string();
	}

	static unique_ptr<FileHandle> OpenWriter(Connection &con) {
		auto &fs = FileSystem::GetFileSystem(*con.context);
		return fs.OpenFile(S3TestHelper::S3_PATH, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}

	static void Configure(DuckDB &db, Connection &con, MockS3Server &server, const string &client_implementation) {
		S3TestHelper::ConfigureRefresh(db, con, server, client_implementation, false);
		S3TestHelper::RequireQueryOk(con, "SET s3_uploader_max_filesize='50GB'");
		S3TestHelper::RequireQueryOk(con, "SET http_retries=0");
	}

	static idx_t Count(const vector<MockS3RequestObservation> &observations, const string &method,
	                   const string &target_contains = string()) {
		idx_t result = 0;
		for (const auto &observation : observations) {
			if (observation.method == method &&
			    (target_contains.empty() || StringUtil::Contains(observation.target, target_contains))) {
				result++;
			}
		}
		return result;
	}

	static vector<MockS3RequestObservation> SuccessfulParts(const vector<MockS3RequestObservation> &observations) {
		vector<MockS3RequestObservation> result;
		for (const auto &observation : observations) {
			if (observation.method == "PUT" && observation.status == 200 && observation.part_number.IsValid()) {
				result.push_back(observation);
			}
		}
		return result;
	}

	static string CreateMultipartPayload() {
		string result(12ULL * 1024ULL * 1024ULL + 1, '\0');
		for (idx_t index = 0; index < result.size(); index++) {
			result[index] = NumericCast<char>('a' + index % 23);
		}
		return result;
	}

	static void RunSinglePut(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		string payload = "single PUT upload baseline";
		handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()), payload.size());
		handle->Close();
		string extra = "x";
		auto write_after_finalize = RequireError(
		    [&]() { handle->Write(QueryContext(*con.context), data_ptr_cast(extra.data()), extra.size()); });
		REQUIRE(StringUtil::Contains(write_after_finalize, "finalized S3 upload"));
		handle->Close();
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "COMMIT");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(server.UploadedObject() == payload);
		REQUIRE(Count(observations, "PUT") == 1);
		REQUIRE(Count(observations, "POST") == 0);
		for (const auto &observation : observations) {
			if (observation.method != "PUT") {
				continue;
			}
			REQUIRE_FALSE(observation.part_number.IsValid());
			REQUIRE(observation.upload_id.empty());
			REQUIRE(observation.body_size == payload.size());
			REQUIRE_FALSE(observation.body_digest.empty());
		}
	}

	static void RunEmpty(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		handle->Sync();
		handle->Close();
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "COMMIT");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(server.UploadedObject().empty());
		REQUIRE(Count(observations, "PUT") == 1);
		REQUIRE(Count(observations, "POST") == 0);
		for (const auto &observation : observations) {
			if (observation.method == "PUT") {
				REQUIRE(observation.body_size == 0);
				REQUIRE_FALSE(observation.part_number.IsValid());
			}
		}
	}

	static void RunMultipart(const string &client_implementation) {
		const string upload_id = "upload-baseline-id";
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		config.upload.upload_id = upload_id;
		config.upload.blocked_part_numbers = {1};
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		auto payload = CreateMultipartPayload();
		std::exception_ptr write_error;
		std::thread writer([&]() {
			try {
				handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()), payload.size());
			} catch (...) {
				write_error = std::current_exception();
			}
		});
		auto part_1_started = server.WaitForPartUpload(1);
		auto concurrent_finalize_error = RequireError([&]() { handle->Close(); });
		auto cursor_future = std::async(std::launch::async, [&]() { return handle->SeekPosition(); });
		auto cursor_available = cursor_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
		server.ReleasePartUploads();
		writer.join();
		auto cursor_position = cursor_future.get();
		if (write_error) {
			std::rethrow_exception(write_error);
		}

		auto observed_concurrency = server.MaximumConcurrentPartUploads();
		handle->Close();
		handle->Close();
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "COMMIT");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(part_1_started);
		REQUIRE(StringUtil::Contains(concurrent_finalize_error, "Concurrent S3 upload operations"));
		REQUIRE(cursor_available);
		REQUIRE(cursor_position == 0);
		REQUIRE(observed_concurrency == 1);
		REQUIRE(server.UploadedObject() == payload);
		REQUIRE(Count(observations, "POST", "uploads") == 1);
		REQUIRE(Count(observations, "POST", "uploadId") == 1);
		REQUIRE(Count(observations, "PUT", "partNumber") == 3);

		auto parts = SuccessfulParts(observations);
		REQUIRE(parts.size() == 3);
		vector<idx_t> part_sizes(3);
		for (const auto &part : parts) {
			REQUIRE(part.upload_id == upload_id);
			auto part_number = part.part_number.GetIndex();
			REQUIRE(part_number >= 1);
			REQUIRE(part_number <= part_sizes.size());
			part_sizes[part_number - 1] = part.body_size;
			REQUIRE_FALSE(part.body_digest.empty());
		}
		REQUIRE(part_sizes[0] >= MINIMUM_PART_SIZE);
		REQUIRE(part_sizes[1] == part_sizes[0]);
		REQUIRE(part_sizes[2] == payload.size() - part_sizes[0] - part_sizes[1]);
		REQUIRE(part_sizes[2] < part_sizes[0]);

		const string expected_completion_body =
		    "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
		    "<Part><ETag>\"mock-part-1\"</ETag><PartNumber>1</PartNumber></Part>"
		    "<Part><ETag>\"mock-part-2\"</ETag><PartNumber>2</PartNumber></Part>"
		    "<Part><ETag>\"mock-part-3\"</ETag><PartNumber>3</PartNumber></Part>"
		    "</CompleteMultipartUpload>";
		REQUIRE(server.CompletionBody() == expected_completion_body);
		for (const auto &observation : observations) {
			if (observation.method == "POST" && StringUtil::Contains(observation.target, "uploadId")) {
				REQUIRE(observation.upload_id == upload_id);
			}
		}
	}

	static void RunFragmentedMultipart(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		auto payload = CreateMultipartPayload();
		const vector<idx_t> write_sizes {1, MINIMUM_PART_SIZE - 2, 3, MINIMUM_PART_SIZE,
		                                 payload.size() - 2 * MINIMUM_PART_SIZE - 2};
		idx_t offset = 0;
		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		for (auto write_size : write_sizes) {
			handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()) + offset, write_size);
			offset += write_size;
		}
		REQUIRE(offset == payload.size());
		handle->Close();
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "COMMIT");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(server.UploadedObject() == payload);
		REQUIRE(Count(observations, "POST", "uploads") == 1);
		REQUIRE(Count(observations, "POST", "uploadId") == 1);
		REQUIRE(Count(observations, "PUT", "partNumber") == 3);
	}

	static void RunAsyncWriter(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);
		S3TestHelper::RequireQueryOk(con, "SET async_threads=2");

		auto payload = CreateMultipartPayload();
		auto &fs = FileSystem::GetFileSystem(*con.context);
		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		AsyncFileWriter writer(*con.context, fs, S3TestHelper::S3_PATH,
		                       FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		writer.WriteData(const_data_ptr_cast(payload.data()), payload.size());
		writer.Close();
		S3TestHelper::RequireQueryOk(con, "COMMIT");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(server.UploadedObject() == payload);
		REQUIRE(server.MaximumConcurrentPartUploads() == 1);
		REQUIRE(Count(observations, "POST", "uploads") == 1);
		REQUIRE(Count(observations, "POST", "uploadId") == 1);
	}

	static void RunStableSinglePutFailure(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		config.failures.transient_put_failures = 1000;
		config.failures.failure_is_request_timeout = false;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		string payload = "failed single PUT";
		handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()), payload.size());
		auto first_error = RequireError([&]() { handle->Sync(); });
		auto second_error = RequireError([&]() { handle->Close(); });
		REQUIRE(second_error == first_error);
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(Count(observations, "PUT") == 1);
		REQUIRE(Count(observations, "POST") == 0);
	}

	static void RunStableCompletionFailure(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		config.failures.transient_complete_post_failures = 1000;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		auto payload = CreateMultipartPayload();
		handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()), payload.size());
		auto first_error = RequireError([&]() { handle->Sync(); });
		auto second_error = RequireError([&]() { handle->Close(); });
		REQUIRE(second_error == first_error);
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(Count(observations, "POST", "uploads") == 1);
		REQUIRE(Count(observations, "POST", "uploadId") == 1);
		REQUIRE(Count(observations, "PUT", "partNumber") == 3);
		REQUIRE(Count(observations, "DELETE") == 0);
	}

	static void RunStablePartFailure(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		config.failures.transient_put_failures = 1000;
		config.failures.failure_is_request_timeout = false;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		auto payload = CreateMultipartPayload();
		auto first_error = RequireError(
		    [&]() { handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()), payload.size()); });
		auto second_error = RequireError([&]() { handle->Close(); });
		REQUIRE(second_error == first_error);
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(Count(observations, "POST", "uploads") == 1);
		REQUIRE(Count(observations, "POST", "uploadId") == 0);
		REQUIRE(Count(observations, "PUT", "partNumber") == 1);
		REQUIRE(Count(observations, "DELETE") == 0);
	}

	static void RunOutOfOrderFailure(const string &client_implementation) {
		MockS3ServerConfig config;
		config.object.bucket = S3TestHelper::BUCKET;
		config.object.key = S3TestHelper::OBJECT_KEY;
		config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		Configure(db, con, server, client_implementation);

		S3TestHelper::RequireQueryOk(con, "BEGIN TRANSACTION");
		auto handle = OpenWriter(con);
		string payload = "out of order";
		auto first_error = RequireError(
		    [&]() { handle->Write(QueryContext(*con.context), data_ptr_cast(payload.data()), payload.size(), 1); });
		auto second_error = RequireError([&]() { handle->Close(); });
		REQUIRE(second_error == first_error);
		handle.reset();
		S3TestHelper::RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(Count(observations, "PUT") == 0);
		REQUIRE(Count(observations, "POST") == 0);
	}

	static void Run(const string &client_implementation) {
		SECTION("empty PUT") {
			RunEmpty(client_implementation);
		}
		SECTION("single PUT") {
			RunSinglePut(client_implementation);
		}
		SECTION("multipart upload") {
			RunMultipart(client_implementation);
		}
		SECTION("fragmented multipart upload") {
			RunFragmentedMultipart(client_implementation);
		}
		SECTION("async writer multipart upload") {
			RunAsyncWriter(client_implementation);
		}
		SECTION("stable single PUT failure") {
			RunStableSinglePutFailure(client_implementation);
		}
		SECTION("stable completion failure") {
			RunStableCompletionFailure(client_implementation);
		}
		SECTION("stable part failure") {
			RunStablePartFailure(client_implementation);
		}
		SECTION("out-of-order failure") {
			RunOutOfOrderFailure(client_implementation);
		}
	}
};

} // namespace

TEST_CASE("S3 upload behavior baseline", "[httpfs][s3][upload]") {
	SECTION("curl") {
		S3UploadBaselineTest::Run("curl");
	}
	SECTION("httplib") {
		S3UploadBaselineTest::Run("httplib");
	}
}

TEST_CASE("S3 upload threads are scheduled by DuckDB", "[httpfs][s3][upload]") {
	DuckDB db(nullptr);
	S3TestHelper::LoadExtension(db);
	Connection con(db);
	auto result = con.Query("SELECT count(*) FROM duckdb_settings() WHERE name = 's3_uploader_thread_limit'");
	REQUIRE(result);
	REQUIRE_FALSE(result->HasError());
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 0);
}

} // namespace duckdb
