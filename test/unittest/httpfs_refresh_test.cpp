#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "create_secret_functions.hpp"
#include "httpfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"

#include <atomic>
#include <mutex>
#include <unordered_map>

namespace duckdb {

namespace {

static constexpr const char *STALE_KEY_ID = "STALE_KEY";
static constexpr const char *FRESH_KEY_ID = "FRESH_KEY";
static constexpr const char *STALE_SECRET = "STALE_SECRET";
static constexpr const char *FRESH_SECRET = "FRESH_SECRET";
static constexpr const char *TEST_PROVIDER = "httpfs_refresh_test";
static constexpr const char *BUCKET = "refresh-bucket";
static constexpr const char *OBJECT_KEY = "object.bin";
static constexpr const char *S3_PATH = "s3://refresh-bucket/object.bin";

struct ProviderStats {
	idx_t initial_creations = 0;
	idx_t refresh_creations = 0;
	vector<string> key_ids;
};

struct ProviderRegistry {
	std::mutex lock;
	std::unordered_map<string, ProviderStats> stats;
};

static ProviderRegistry &GetProviderRegistry() {
	static ProviderRegistry registry;
	return registry;
}

static string GetOptionString(const CreateSecretInput &input, const string &key) {
	auto entry = input.options.find(key);
	if (entry == input.options.end() || entry->second.IsNull()) {
		return string();
	}
	return entry->second.ToString();
}

static void RecordProviderCall(const string &test_id, const string &key_id) {
	if (test_id.empty()) {
		return;
	}
	auto &registry = GetProviderRegistry();
	std::lock_guard<std::mutex> lock(registry.lock);
	auto &stats = registry.stats[test_id];
	stats.key_ids.push_back(key_id);
	if (key_id == STALE_KEY_ID) {
		stats.initial_creations++;
	} else if (key_id == FRESH_KEY_ID) {
		stats.refresh_creations++;
	}
}

static ProviderStats GetProviderStats(const string &test_id) {
	auto &registry = GetProviderRegistry();
	std::lock_guard<std::mutex> lock(registry.lock);
	auto entry = registry.stats.find(test_id);
	if (entry == registry.stats.end()) {
		return ProviderStats();
	}
	return entry->second;
}

struct TestS3SecretFunctions : public CreateS3SecretFunctions {
	static void SetTestNamedParams(CreateSecretFunction &function, string type) {
		SetBaseNamedParams(function, type);
		function.named_parameters["test_id"] = LogicalType::VARCHAR;
	}

	static unique_ptr<BaseSecret> CreateTestSecret(ClientContext &context, CreateSecretInput &input) {
		RecordProviderCall(GetOptionString(input, "test_id"), GetOptionString(input, "key_id"));

		auto delegated_input = input;
		auto test_id_entry = delegated_input.options.find("test_id");
		if (test_id_entry != delegated_input.options.end()) {
			delegated_input.options.erase(test_id_entry);
		}
		return CreateSecretFunctionInternal(context, delegated_input);
	}
};

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

static void RegisterRefreshTestProvider(DuckDB &db) {
	ExtensionInfo extension_info;
	ExtensionActiveLoad load_info(*db.instance, extension_info, "httpfs_refresh_test");
	ExtensionLoader loader(load_info);

	CreateSecretFunction function;
	function.secret_type = "s3";
	function.provider = TEST_PROVIDER;
	function.function = TestS3SecretFunctions::CreateTestSecret;
	TestS3SecretFunctions::SetTestNamedParams(function, "s3");
	loader.RegisterFunction(function);
}

static void RequireQueryOk(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE(result);
	INFO((result->HasError() ? result->GetError() : string()));
	REQUIRE_FALSE(result->HasError());
}

static string NextTestId() {
	static std::atomic<idx_t> next_id(0);
	return StringUtil::Format("httpfs_refresh_test_%llu", static_cast<unsigned long long>(++next_id));
}

static string ConfigureRefreshTest(DuckDB &db, Connection &con, MockS3Server &server,
                                   const string &client_implementation, bool connection_caching) {
	auto test_id = NextTestId();

	LoadHTTPFSExtension(db);
	RegisterRefreshTestProvider(db);

	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, StringUtil::Format("SET httpfs_connection_caching=%s", connection_caching ? "true" : "false"));
	RequireQueryOk(con, StringUtil::Format("SET s3_endpoint='%s'", server.Endpoint()));
	RequireQueryOk(con, "SET s3_region='us-east-1'");
	RequireQueryOk(con, "SET s3_use_ssl=false");
	RequireQueryOk(con, "SET s3_url_style='path'");

	RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET refresh_s3 (
	TYPE S3,
	PROVIDER %s,
	SCOPE 's3://refresh-bucket/',
	KEY_ID '%s',
	SECRET '%s',
	TEST_ID '%s',
	REFRESH_INFO MAP {
		'KEY_ID': '%s',
		'SECRET': '%s',
		'TEST_ID': '%s'
	}
))",
	                                       TEST_PROVIDER, STALE_KEY_ID, STALE_SECRET, test_id, FRESH_KEY_ID,
	                                       FRESH_SECRET, test_id));
	return test_id;
}

static void AssertSingleRefresh(const string &test_id) {
	auto stats = GetProviderStats(test_id);
	REQUIRE(stats.initial_creations == 1);
	REQUIRE(stats.refresh_creations == 1);
}

template <class OPERATION>
static vector<MockS3RequestObservation> RunRefreshScenario(MockS3RefreshTarget refresh_target,
                                                           const string &client_implementation, bool connection_caching,
                                                           OPERATION operation) {
	MockS3ServerConfig config;
	config.bucket = BUCKET;
	config.object_key = OBJECT_KEY;
	config.stale_key_id = STALE_KEY_ID;
	config.refresh_target = refresh_target;
	auto object_data = config.object_data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, client_implementation, connection_caching);
	INFO(StringUtil::Format("refresh target: %s, client: %s, connection caching: %s",
	                        MockS3RefreshTargetName(refresh_target), client_implementation,
	                        connection_caching ? "true" : "false"));
	RequireQueryOk(con, "BEGIN TRANSACTION");
	operation(con, object_data);
	RequireQueryOk(con, "COMMIT");
	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	AssertSingleRefresh(test_id);
	return observations;
}

static void OpenForRead(Connection &con, FileOpenFlags flags = FileFlags::FILE_FLAGS_READ) {
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, flags);
	REQUIRE(handle);
}

static void RunHeadRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(
	    MockS3RefreshTarget::HEAD, client_implementation, connection_caching, [](Connection &con, const string &) {
		    OpenForRead(con, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	    });
	REQUIRE(MockS3HasObservation(observations, "HEAD", STALE_KEY_ID, 403));
	REQUIRE(MockS3HasObservation(observations, "HEAD", FRESH_KEY_ID, 200));
}

static void RunFullGetRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(MockS3RefreshTarget::FULL_GET, client_implementation, connection_caching,
	                                       [](Connection &con, const string &object_data) {
		                                       RequireQueryOk(con, "SET force_download=true");
		                                       auto &fs = FileSystem::GetFileSystem(*con.context);
		                                       auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ);
		                                       string buffer(8, '\0');
		                                       handle->Read(QueryContext(*con.context), &buffer[0], buffer.size(), 0);
		                                       REQUIRE(buffer == object_data.substr(0, buffer.size()));
	                                       });
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 200));
}

static void RunRangeRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(
	    MockS3RefreshTarget::RANGE_GET, client_implementation, connection_caching,
	    [](Connection &con, const string &object_data) {
		    auto &fs = FileSystem::GetFileSystem(*con.context);
		    auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);

		    const idx_t first_offset = 7;
		    const idx_t first_read_size = 9;
		    string first_buffer(first_read_size, '\0');
		    handle->Read(QueryContext(*con.context), &first_buffer[0], first_read_size, first_offset);
		    REQUIRE(first_buffer == object_data.substr(first_offset, first_read_size));

		    const idx_t second_offset = 20;
		    const idx_t second_read_size = 5;
		    string second_buffer(second_read_size, '\0');
		    handle->Read(QueryContext(*con.context), &second_buffer[0], second_read_size, second_offset);
		    REQUIRE(second_buffer == object_data.substr(second_offset, second_read_size));
	    });
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403, "bytes=7-15"));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 206, "bytes=7-15"));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 206, "bytes=20-24"));
	REQUIRE_FALSE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403, "bytes=20-24"));
}

static void RunPutRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(
	    MockS3RefreshTarget::PUT, client_implementation, connection_caching, [](Connection &con, const string &) {
		    auto &fs = FileSystem::GetFileSystem(*con.context);
		    auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		    string payload = "hello from httpfs refresh";
		    handle->Write(QueryContext(*con.context), &payload[0], payload.size());
		    handle->Close();
	    });
	REQUIRE(MockS3HasObservation(observations, "PUT", STALE_KEY_ID, 403));
	REQUIRE(MockS3HasObservation(observations, "PUT", FRESH_KEY_ID, 200));
}

static void WriteMultipartPayload(Connection &con) {
	RequireQueryOk(con, "SET s3_uploader_max_filesize='50GB'");
	RequireQueryOk(con, "SET s3_uploader_thread_limit=1");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	string payload(10 * 1024 * 1024 + 1, 'x');
	handle->Write(QueryContext(*con.context), &payload[0], payload.size());
	handle->Close();
}

static void RunMultipartInitiatePostRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations =
	    RunRefreshScenario(MockS3RefreshTarget::MULTIPART_INITIATE_POST, client_implementation, connection_caching,
	                       [](Connection &con, const string &) { WriteMultipartPayload(con); });
	REQUIRE(MockS3HasObservation(observations, "POST", STALE_KEY_ID, 403, string(), "uploads"));
	REQUIRE(MockS3HasObservation(observations, "POST", FRESH_KEY_ID, 200, string(), "uploads"));
	REQUIRE(MockS3HasObservation(observations, "POST", FRESH_KEY_ID, 200, string(), "uploadId"));
}

static void RunMultipartCompletePostRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations =
	    RunRefreshScenario(MockS3RefreshTarget::MULTIPART_COMPLETE_POST, client_implementation, connection_caching,
	                       [](Connection &con, const string &) { WriteMultipartPayload(con); });
	REQUIRE(MockS3HasObservation(observations, "POST", STALE_KEY_ID, 403, string(), "uploadId"));
	REQUIRE(MockS3HasObservation(observations, "POST", FRESH_KEY_ID, 200, string(), "uploadId"));
}

static void RunBulkDeletePostRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(MockS3RefreshTarget::BULK_DELETE_POST, client_implementation,
	                                       connection_caching, [](Connection &con, const string &) {
		                                       auto &fs = FileSystem::GetFileSystem(*con.context);
		                                       vector<string> paths;
		                                       paths.push_back(S3_PATH);
		                                       fs.RemoveFiles(paths);
	                                       });
	REQUIRE(MockS3HasObservation(observations, "POST", STALE_KEY_ID, 403, string(), "delete"));
	REQUIRE(MockS3HasObservation(observations, "POST", FRESH_KEY_ID, 200, string(), "delete"));
}

static void RunDeleteRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(MockS3RefreshTarget::DELETE, client_implementation, connection_caching,
	                                       [](Connection &con, const string &) {
		                                       auto &fs = FileSystem::GetFileSystem(*con.context);
		                                       fs.RemoveFile(S3_PATH);
	                                       });
	REQUIRE(MockS3HasObservation(observations, "DELETE", STALE_KEY_ID, 403));
	REQUIRE(MockS3HasObservation(observations, "DELETE", FRESH_KEY_ID, 204));
}

static void RunListGlobRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations =
	    RunRefreshScenario(MockS3RefreshTarget::LIST_OBJECTS_GET, client_implementation, connection_caching,
	                       [](Connection &con, const string &) {
		                       auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/object*.bin')");
		                       REQUIRE(result);
		                       INFO((result->HasError() ? result->GetError() : string()));
		                       REQUIRE_FALSE(result->HasError());
		                       REQUIRE(result->RowCount() == 1);
		                       REQUIRE(result->GetValue(0, 0).ToString() == S3_PATH);
	                       });
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403, string(), "list-type=2"));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 200, string(), "list-type=2"));
}

static void RunAllRequestRefreshScenarios(const string &client_implementation, bool connection_caching) {
	RunHeadRefreshScenario(client_implementation, connection_caching);
	RunFullGetRefreshScenario(client_implementation, connection_caching);
	RunRangeRefreshScenario(client_implementation, connection_caching);
	RunPutRefreshScenario(client_implementation, connection_caching);
	RunMultipartInitiatePostRefreshScenario(client_implementation, connection_caching);
	RunMultipartCompletePostRefreshScenario(client_implementation, connection_caching);
	RunBulkDeletePostRefreshScenario(client_implementation, connection_caching);
	RunDeleteRefreshScenario(client_implementation, connection_caching);
	RunListGlobRefreshScenario(client_implementation, connection_caching);
}

static void RunHandleRequestRefreshScenarios(const string &client_implementation, bool connection_caching) {
	RunHeadRefreshScenario(client_implementation, connection_caching);
	RunFullGetRefreshScenario(client_implementation, connection_caching);
	RunRangeRefreshScenario(client_implementation, connection_caching);
	RunDeleteRefreshScenario(client_implementation, connection_caching);
}

} // namespace

TEST_CASE("HTTPFS refreshes S3 credentials across request methods", "[httpfs][s3][refresh]") {
	SECTION("httplib without connection caching") {
		RunAllRequestRefreshScenarios("httplib", false);
	}
	SECTION("httplib with handle client cache reuse") {
		RunHandleRequestRefreshScenarios("httplib", false);
	}
	SECTION("curl without connection caching") {
		RunAllRequestRefreshScenarios("curl", false);
	}
}

} // namespace duckdb
