#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "create_secret_functions.hpp"
#include "httpfs_client.hpp"
#include "httpfs_extension.hpp"
#include "s3fs.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <array>
#include <atomic>
#include <functional>
#include <mutex>
#include <new>
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
	annotated_mutex lock;
	std::unordered_map<string, ProviderStats> stats DUCKDB_GUARDED_BY(lock);
	std::unordered_map<string, std::function<void()>> refresh_hooks DUCKDB_GUARDED_BY(lock);
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
	std::function<void()> refresh_hook;
	{
		annotated_lock_guard<annotated_mutex> lock(registry.lock);
		auto &stats = registry.stats[test_id];
		stats.key_ids.push_back(key_id);
		if (key_id == STALE_KEY_ID) {
			stats.initial_creations++;
		} else if (key_id == FRESH_KEY_ID) {
			stats.refresh_creations++;
			auto entry = registry.refresh_hooks.find(test_id);
			if (entry != registry.refresh_hooks.end()) {
				refresh_hook = entry->second;
			}
		}
	}
	if (refresh_hook) {
		refresh_hook();
	}
}

static ProviderStats GetProviderStats(const string &test_id) {
	auto &registry = GetProviderRegistry();
	annotated_lock_guard<annotated_mutex> lock(registry.lock);
	auto entry = registry.stats.find(test_id);
	if (entry == registry.stats.end()) {
		return ProviderStats();
	}
	return entry->second;
}

static void SetProviderRefreshHook(const string &test_id, std::function<void()> refresh_hook) {
	auto &registry = GetProviderRegistry();
	annotated_lock_guard<annotated_mutex> lock(registry.lock);
	registry.refresh_hooks[test_id] = std::move(refresh_hook);
}

static void ClearProviderRefreshHook(const string &test_id) {
	auto &registry = GetProviderRegistry();
	annotated_lock_guard<annotated_mutex> lock(registry.lock);
	registry.refresh_hooks.erase(test_id);
}

class ProviderRefreshHook {
public:
	ProviderRefreshHook(string test_id_p, std::function<void()> refresh_hook) : test_id(std::move(test_id_p)) {
		SetProviderRefreshHook(test_id, std::move(refresh_hook));
	}
	~ProviderRefreshHook() {
		ClearProviderRefreshHook(test_id);
	}

private:
	string test_id;
};

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
	ExtensionActiveLoad load_info(*db.instance, extension_info, "httpfs", "");
	ExtensionLoader loader(load_info);
	HttpfsExtension extension;
	extension.Load(loader);
}

static void RegisterRefreshTestProvider(DuckDB &db) {
	ExtensionInfo extension_info;
	ExtensionActiveLoad load_info(*db.instance, extension_info, "httpfs_refresh_test", "");
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

static CreateSecretInput CreateTransactionRefreshInput(const string &test_id) {
	CreateSecretInput input;
	input.type = "s3";
	input.provider = TEST_PROVIDER;
	input.name = "transaction_refresh_s3";
	input.scope = {"s3://refresh-bucket/"};
	input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	input.persist_type = SecretPersistType::TRANSACTION;
	input.options["key_id"] = STALE_KEY_ID;
	input.options["secret"] = STALE_SECRET;
	input.options["test_id"] = test_id;

	InsertionOrderPreservingMap<string> refresh_info;
	refresh_info.insert("key_id", FRESH_KEY_ID);
	refresh_info.insert("secret", FRESH_SECRET);
	refresh_info.insert("test_id", test_id);
	input.options["refresh_info"] = Value::MAP(refresh_info);
	return input;
}

static string ConfigureRefreshTest(DuckDB &db, Connection &con, MockS3Server &server,
                                   const string &client_implementation, bool connection_caching,
                                   bool refresh_enabled = true) {
	auto test_id = NextTestId();

	LoadHTTPFSExtension(db);
	RegisterRefreshTestProvider(db);

	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, StringUtil::Format("SET httpfs_connection_caching=%s", connection_caching ? "true" : "false"));
	RequireQueryOk(con,
	               StringUtil::Format("SET httpfs_enable_credential_refresh=%s", refresh_enabled ? "true" : "false"));
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

static void ConfigureEndpointRefreshTest(DuckDB &db, Connection &con, MockS3Server &stale_server,
                                         MockS3Server &fresh_server, const string &client_implementation) {
	auto test_id = NextTestId();

	LoadHTTPFSExtension(db);
	RegisterRefreshTestProvider(db);

	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, "SET httpfs_connection_caching=false");
	RequireQueryOk(con, "SET httpfs_enable_credential_refresh=true");
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
	ENDPOINT '%s',
	TEST_ID '%s',
	REFRESH_INFO MAP {
		'KEY_ID': '%s',
		'SECRET': '%s',
		'ENDPOINT': '%s',
		'TEST_ID': '%s'
	}
))",
	                                       TEST_PROVIDER, STALE_KEY_ID, STALE_SECRET, stale_server.Endpoint(), test_id,
	                                       STALE_KEY_ID, STALE_SECRET, fresh_server.Endpoint(), test_id));
}

static void AssertSingleRefresh(const string &test_id) {
	auto stats = GetProviderStats(test_id);
	REQUIRE(stats.initial_creations == 1);
	REQUIRE(stats.refresh_creations == 1);
}

static void AssertNoRefresh(const string &test_id) {
	auto stats = GetProviderStats(test_id);
	REQUIRE(stats.initial_creations == 1);
	REQUIRE(stats.refresh_creations == 0);
}

static bool HasRequestWithKey(const vector<MockS3RequestObservation> &observations, const string &key_id) {
	for (auto &observation : observations) {
		if (observation.key_id == key_id) {
			return true;
		}
	}
	return false;
}

static idx_t CountObservations(const vector<MockS3RequestObservation> &observations, const string &method,
                               const string &key_id, int status) {
	idx_t result = 0;
	for (auto &observation : observations) {
		if (observation.method == method && observation.key_id == key_id && observation.status == status) {
			result++;
		}
	}
	return result;
}

static vector<int> ObservationPorts(const vector<MockS3RequestObservation> &observations, const string &method,
                                    const string &key_id, int status, const string &range = string()) {
	vector<int> result;
	for (auto &observation : observations) {
		if (observation.method == method && observation.key_id == key_id && observation.status == status &&
		    observation.range == range) {
			result.push_back(observation.remote_port);
		}
	}
	return result;
}

template <class OPERATION>
static vector<MockS3RequestObservation>
RunRefreshScenario(MockS3RefreshTarget refresh_target, const string &client_implementation, bool connection_caching,
                   OPERATION operation, int stale_auth_status = 403, string stale_auth_error_code = "AccessDenied") {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.stale_status = stale_auth_status;
	config.auth.stale_error_code = std::move(stale_auth_error_code);
	config.auth.refresh_target = refresh_target;
	auto object_data = config.object.data;
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

// A payload small enough to be a single-shot PUT (no multipart), so the S3 request loop is the only
// retry layer and the PUT count reflects exactly that loop's behavior.
static void WriteSinglePutPayload(Connection &con) {
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	string payload = "single-shot payload";
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

static void RunBulkDeleteEndpointRefreshScenario(const string &client_implementation) {
	MockS3ServerConfig stale_config;
	stale_config.object.bucket = BUCKET;
	stale_config.object.key = OBJECT_KEY;
	stale_config.auth.stale_key_id = STALE_KEY_ID;
	stale_config.auth.refresh_target = MockS3RefreshTarget::BULK_DELETE_POST;
	MockS3Server stale_server(std::move(stale_config));

	MockS3ServerConfig fresh_config;
	fresh_config.object.bucket = BUCKET;
	fresh_config.object.key = OBJECT_KEY;
	fresh_config.auth.stale_key_id = "NEVER_STALE";
	fresh_config.auth.refresh_target = MockS3RefreshTarget::BULK_DELETE_POST;
	MockS3Server fresh_server(std::move(fresh_config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureEndpointRefreshTest(db, con, stale_server, fresh_server, client_implementation);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	fs.RemoveFiles({S3_PATH});
	RequireQueryOk(con, "COMMIT");

	auto stale_observations = stale_server.Observations();
	auto fresh_observations = fresh_server.Observations();
	INFO(MockS3DescribeObservations(stale_observations));
	INFO(MockS3DescribeObservations(fresh_observations));
	REQUIRE(CountObservations(stale_observations, "POST", STALE_KEY_ID, 403) == 1);
	REQUIRE(CountObservations(fresh_observations, "POST", STALE_KEY_ID, 200) == 1);
}

static void RunDeleteRefreshScenario(const string &client_implementation, bool connection_caching) {
	auto observations = RunRefreshScenario(MockS3RefreshTarget::DELETE_OBJECT, client_implementation,
	                                       connection_caching, [](Connection &con, const string &) {
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

static void RunTokenFullGetRefreshScenario(const string &client_implementation, const string &error_code) {
	auto observations = RunRefreshScenario(
	    MockS3RefreshTarget::FULL_GET, client_implementation, false,
	    [](Connection &con, const string &) {
		    RequireQueryOk(con, "SET force_download=true");
		    auto &fs = FileSystem::GetFileSystem(*con.context);
		    auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ);
		    REQUIRE(handle);
	    },
	    400, error_code);
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 400));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 200));
}

static void RunTokenListRefreshScenario(const string &client_implementation, const string &error_code) {
	auto observations = RunRefreshScenario(
	    MockS3RefreshTarget::LIST_OBJECTS_GET, client_implementation, false,
	    [](Connection &con, const string &) {
		    auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/object*.bin')");
		    REQUIRE(result);
		    INFO((result->HasError() ? result->GetError() : string()));
		    REQUIRE_FALSE(result->HasError());
		    REQUIRE(result->RowCount() == 1);
	    },
	    400, error_code);
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 400, string(), "list-type=2"));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 200, string(), "list-type=2"));
}

static void RunNonAuth400NoRefreshScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.stale_status = 400;
	config.auth.stale_error_code = "InvalidRequest";
	config.auth.refresh_target = MockS3RefreshTarget::FULL_GET;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, client_implementation, false);
	RequireQueryOk(con, "SET force_download=true");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	string error;
	try {
		auto &fs = FileSystem::GetFileSystem(*con.context);
		fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		error = ex.what();
	}
	INFO(error);
	REQUIRE(error.find("InvalidRequest") != string::npos);
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 400));
	REQUIRE_FALSE(HasRequestWithKey(observations, FRESH_KEY_ID));
	AssertNoRefresh(test_id);
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

static void RunRangeRefreshDisabledScenario() {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::RANGE_GET;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, "httplib", false, false);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);

	const idx_t offset = 7;
	const idx_t read_size = 9;
	string buffer(read_size, '\0');
	REQUIRE_THROWS(handle->Read(QueryContext(*con.context), &buffer[0], read_size, offset));
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403, "bytes=7-15"));
	REQUIRE_FALSE(HasRequestWithKey(observations, FRESH_KEY_ID));
	AssertNoRefresh(test_id);
}

static void RunFullGetErrorBodyScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::FULL_GET;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, client_implementation, false, false);

	RequireQueryOk(con, "SET force_download=true");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	string error;
	try {
		auto &fs = FileSystem::GetFileSystem(*con.context);
		fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		error = ex.what();
	}
	INFO(error);
	REQUIRE(error.find("AccessDenied: stale credentials") != string::npos);
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403));
	REQUIRE_FALSE(HasRequestWithKey(observations, FRESH_KEY_ID));
	AssertNoRefresh(test_id);
}

static void RunChunkedFullGetScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.full_get.chunked = true;
	auto object_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET force_download=true");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ);
	string result(object_data.size(), '\0');
	handle->Read(QueryContext(*con.context), &result[0], result.size(), 0);
	REQUIRE(result == object_data);
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 200));
	AssertNoRefresh(test_id);
}

static void RunListGlobRefreshDisabledScenario() {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::LIST_OBJECTS_GET;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, "httplib", false, false);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto result = con.Query("SELECT file FROM glob('s3://refresh-bucket/object*.bin')");
	REQUIRE(result);
	REQUIRE(result->HasError());
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403, string(), "list-type=2"));
	REQUIRE_FALSE(HasRequestWithKey(observations, FRESH_KEY_ID));
	AssertNoRefresh(test_id);
}

static void RunS3HeaderScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.auth.stale_key_id = "NEVER_STALE";
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	LoadHTTPFSExtension(db);
	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, "SET httpfs_connection_caching=false");
	RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET s3_headers (
	TYPE S3,
	SCOPE '%s',
	KEY_ID 'FRESH_KEY',
	SECRET 'FRESH_SECRET',
	REGION 'us-east-1',
	ENDPOINT '%s',
	USE_SSL false,
	URL_STYLE 'path',
	EXTRA_HTTP_HEADERS MAP {'X-HTTPFS-Session': 'present'}
))",
	                                       S3_PATH, server.Endpoint()));

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	{
		auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
		REQUIRE(handle);
	}
	RequireQueryOk(con, "COMMIT");

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
	LoadHTTPFSExtension(db);
	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, "SET httpfs_connection_caching=false");
	RequireQueryOk(con, "SET enable_logging=true");
	RequireQueryOk(con, StringUtil::Format(R"(
CREATE SECRET s3_region_redirect (
	TYPE S3,
	SCOPE 's3://refresh-bucket/',
	KEY_ID 'FRESH_KEY',
	SECRET 'FRESH_SECRET',
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
		RequireQueryOk(con, "BEGIN TRANSACTION");
		auto &fs = FileSystem::GetFileSystem(*con.context);
		{
			auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
			REQUIRE(handle);
		}
		RequireQueryOk(con, "COMMIT");
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

static void RunMultipleStaleHandlesSingleRefreshScenario() {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::RANGE_GET;
	auto object_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, "httplib", false);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	vector<unique_ptr<FileHandle>> handles;
	for (idx_t i = 0; i < 4; i++) {
		handles.push_back(fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO));
		REQUIRE(handles.back());
	}

	const idx_t offset = 7;
	const idx_t read_size = 9;
	for (auto &handle : handles) {
		string buffer(read_size, '\0');
		handle->Read(QueryContext(*con.context), &buffer[0], read_size, offset);
		REQUIRE(buffer == object_data.substr(offset, read_size));
	}
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservations(observations, "GET", STALE_KEY_ID, 403) >= handles.size());
	REQUIRE(CountObservations(observations, "GET", FRESH_KEY_ID, 206) == handles.size());
	AssertSingleRefresh(test_id);
}

static void PublishTestS3Region(const shared_ptr<HTTPRequestSession> &session, const string &region) {
	for (;;) {
		auto captured = session->Capture();
		auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
		if (snapshot.auth_params.region == region) {
			return;
		}
		auto auth_params = snapshot.auth_params;
		auth_params.SetRegion(region);
		auto http_params = snapshot.CreateRequestParams();
		auto replacement = make_shared_ptr<S3RequestSnapshot>(
		    *http_params, auth_params, snapshot.refresh_path, snapshot.client_context,
		    snapshot.credential_refresh_enabled, true, snapshot.credential_generation);
		if (session->TryPublish(captured.snapshot, std::move(replacement)).published) {
			return;
		}
	}
}

static void RunRefreshPublicationScenario(const string &client_implementation, bool invalidate_clients,
                                          bool publish_region) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::RANGE_GET;
	auto object_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	auto test_id = ConfigureRefreshTest(db, con, server, client_implementation, false);
	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(handle);
	auto session = handle->Cast<S3FileHandle>().request_session;
	ProviderRefreshHook refresh_hook(test_id, [session, invalidate_clients, publish_region]() {
		if (invalidate_clients) {
			session->InvalidateClients();
		}
		if (publish_region) {
			PublishTestS3Region(session, "eu-west-1");
		}
	});

	const idx_t offset = 7;
	const idx_t read_size = 9;
	string buffer(read_size, '\0');
	handle->Read(QueryContext(*con.context), &buffer[0], read_size, offset);
	REQUIRE(buffer == object_data.substr(offset, read_size));
	RequireQueryOk(con, "COMMIT");

	auto &snapshot = session->Capture().snapshot->Cast<S3RequestSnapshot>();
	REQUIRE(snapshot.auth_params.access_key_id == FRESH_KEY_ID);
	REQUIRE(snapshot.credential_generation == 1);
	if (publish_region) {
		REQUIRE(snapshot.auth_params.region == "eu-west-1");
		REQUIRE(snapshot.region_redirected);
	}

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 403, "bytes=7-15"));
	REQUIRE(MockS3HasObservation(observations, "GET", FRESH_KEY_ID, 206, "bytes=7-15"));
	if (publish_region) {
		bool saw_merged_request = false;
		for (const auto &observation : observations) {
			if (observation.method == "GET" && observation.key_id == FRESH_KEY_ID &&
			    observation.region == "eu-west-1" && observation.status == 206) {
				saw_merged_request = true;
			}
		}
		REQUIRE(saw_merged_request);
	}
	AssertSingleRefresh(test_id);
}

static void RunTransientPutRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	// Use a refresh target that writes never exercise, so credential refresh never triggers here.
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 2;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	WriteMultipartPayload(con);
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// The transient RequestTimeout 400s were surfaced, retried, and the same part eventually succeeded.
	REQUIRE(MockS3HasObservation(observations, "PUT", STALE_KEY_ID, 400, string(), "partNumber"));
	REQUIRE(MockS3HasObservation(observations, "PUT", STALE_KEY_ID, 200, string(), "partNumber"));
	// The multipart upload completed successfully.
	REQUIRE(MockS3HasObservation(observations, "POST", STALE_KEY_ID, 200, string(), "uploadId"));
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
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1000;
	config.failures.failure_is_request_timeout = false;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(WriteSinglePutPayload(con));
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservations(observations, "PUT", STALE_KEY_ID, 400) == 1);
}

// A truncated error body (an open <Code> with no closing tag) must degrade to a plain HTTP error:
// not classified as transient (no retry) and never escalated to a DB-invalidating InternalException.
static void RunTruncatedErrorBodyScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1000;
	config.failures.truncated_failure_body = true;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	bool threw = false;
	bool threw_internal = false;
	try {
		WriteSinglePutPayload(con);
	} catch (std::exception &ex) {
		threw = true;
		ErrorData error(ex);
		threw_internal = error.Type() == ExceptionType::INTERNAL || error.Type() == ExceptionType::FATAL;
	}
	REQUIRE(threw);
	REQUIRE_FALSE(threw_internal);
	// The database must still be usable (an InternalException would have invalidated it).
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservations(observations, "PUT", STALE_KEY_ID, 400) == 1);
}

// Even a retryable RequestTimeout must NOT replay a multipart-init POST (it would orphan an upload id).
static void RunMultipartInitNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_post_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(WriteMultipartPayload(con));
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservationsTarget(observations, "POST", 400, "uploads") == 1);
}

// A RequestTimeout retry must run on a fresh connection instead of the stalled one.
static void RunFreshConnectionRetryScenario(const string &client_implementation, bool connection_caching) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, connection_caching);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	WriteSinglePutPayload(con);
	RequireQueryOk(con, "COMMIT");

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
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_put_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=2");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(WriteSinglePutPayload(con));
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// One initial attempt plus http_retries (2) retries.
	REQUIRE(CountObservations(observations, "PUT", STALE_KEY_ID, 400) == 3);
}

static void RunTransientGetRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_get_failures = 2;
	auto object_data = config.object.data;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "SET force_download=true");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ);
	string buffer(8, '\0');
	handle->Read(QueryContext(*con.context), &buffer[0], buffer.size(), 0);
	REQUIRE(buffer == object_data.substr(0, buffer.size()));
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	// The read hit transient RequestTimeout 400s and was retried until it succeeded.
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 400));
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 200));
}

static void RunTransientDeleteRetryScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	// Use a refresh target that deletes never exercise, so credential refresh never triggers here.
	config.auth.refresh_target = MockS3RefreshTarget::PUT;
	config.failures.transient_delete_failures = 2;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	fs.RemoveFile(S3_PATH);
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservations(observations, "DELETE", STALE_KEY_ID, 400) == 2);
	REQUIRE(CountObservations(observations, "DELETE", STALE_KEY_ID, 204) == 1);
}

// HEAD responses carry no body, so a RequestTimeout 400 cannot be classified as transient:
// the HEAD is not retried and the open recovers through httpfs's range-GET fallback instead.
static void RunHeadNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_head_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(S3_PATH, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(handle);
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservations(observations, "HEAD", STALE_KEY_ID, 400) == 1);
	REQUIRE(MockS3HasObservation(observations, "GET", STALE_KEY_ID, 206, "bytes=0-1"));
	auto error_ports = ObservationPorts(observations, "HEAD", STALE_KEY_ID, 400);
	auto success_ports = ObservationPorts(observations, "GET", STALE_KEY_ID, 206, "bytes=0-1");
	REQUIRE(error_ports.size() == 1);
	REQUIRE(success_ports.size() == 1);
	REQUIRE(error_ports[0] != 0);
	REQUIRE(error_ports[0] == success_ports[0]);
}

// Like multipart-init, a multipart-complete POST must not be replayed even on RequestTimeout.
static void RunMultipartCompleteNotRetriedScenario(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object.bucket = BUCKET;
	config.object.key = OBJECT_KEY;
	config.auth.stale_key_id = STALE_KEY_ID;
	config.auth.refresh_target = MockS3RefreshTarget::DELETE_OBJECT;
	config.failures.transient_complete_post_failures = 1000;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureRefreshTest(db, con, server, client_implementation, false);

	RequireQueryOk(con, "SET http_retries=3");
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE_THROWS(WriteMultipartPayload(con));
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountObservationsTarget(observations, "POST", 400, "uploadId") == 1);
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
	SECTION("a HEAD 400 is not retried because HEAD responses carry no error body") {
		RunHeadNotRetriedScenario(client_implementation);
	}
	SECTION("retries are bounded by http_retries") {
		RunRetryBudgetScenario(client_implementation);
	}
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

TEST_CASE("HTTPFS preserves transaction-scoped S3 secrets during refresh", "[httpfs][s3][refresh]") {
	DuckDB db(nullptr);
	Connection con(db);
	LoadHTTPFSExtension(db);
	RegisterRefreshTestProvider(db);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto test_id = NextTestId();
	auto input = CreateTransactionRefreshInput(test_id);
	auto &secret_manager = SecretManager::Get(*db.instance);
	auto created_secret = secret_manager.CreateSecret(*con.context, input);
	REQUIRE(created_secret);
	REQUIRE(created_secret->persist_type == SecretPersistType::TRANSACTION);
	REQUIRE(created_secret->storage_mode == SecretManager::TRANSACTION_STORAGE_NAME);

	REQUIRE(CreateS3SecretFunctions::TryRefreshS3Secret(*con.context, *created_secret));
	AssertSingleRefresh(test_id);

	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*con.context);
	auto refreshed_secret = secret_manager.GetSecretByName(transaction, input.name.GetIdentifierName());
	REQUIRE(refreshed_secret);
	REQUIRE(refreshed_secret->persist_type == SecretPersistType::TRANSACTION);
	REQUIRE(refreshed_secret->storage_mode == SecretManager::TRANSACTION_STORAGE_NAME);
	auto secret_string = refreshed_secret->secret->ToString(SecretDisplayType::UNREDACTED);
	REQUIRE(StringUtil::Contains(secret_string, StringUtil::Format("key_id=%s", FRESH_KEY_ID)));
	REQUIRE_FALSE(StringUtil::Contains(secret_string, StringUtil::Format("key_id=%s", STALE_KEY_ID)));

	RequireQueryOk(con, "COMMIT");
	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto next_transaction = CatalogTransaction::GetSystemCatalogTransaction(*con.context);
	REQUIRE_FALSE(secret_manager.GetSecretByName(next_transaction, input.name.GetIdentifierName()));
	RequireQueryOk(con, "ROLLBACK");
}

TEST_CASE("HTTPFS refreshes S3 credentials for token-specific HTTP 400 errors", "[httpfs][s3][refresh]") {
	for (const string client_implementation : {"httplib", "curl"}) {
		DYNAMIC_SECTION(client_implementation << " refreshes ExpiredToken during file open") {
			RunTokenFullGetRefreshScenario(client_implementation, "ExpiredToken");
		}
		DYNAMIC_SECTION(client_implementation << " refreshes InvalidToken during LIST") {
			RunTokenListRefreshScenario(client_implementation, "InvalidToken");
		}
		DYNAMIC_SECTION(client_implementation << " refreshes TokenRefreshRequired during LIST") {
			RunTokenListRefreshScenario(client_implementation, "TokenRefreshRequired");
		}
		DYNAMIC_SECTION(client_implementation << " does not refresh a generic HTTP 400") {
			RunNonAuth400NoRefreshScenario(client_implementation);
		}
	}
}

TEST_CASE("HTTPFS can disable S3 credential refresh", "[httpfs][s3][refresh]") {
	SECTION("range reads fail without refresh") {
		RunRangeRefreshDisabledScenario();
	}
	SECTION("list glob fails without refresh") {
		RunListGlobRefreshDisabledScenario();
	}
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

TEST_CASE("HTTPFS reuses one S3 credential refresh across stale handles", "[httpfs][s3][refresh]") {
	RunMultipleStaleHandlesSingleRefreshScenario();
}

TEST_CASE("S3 credential publication is independent from client invalidation", "[httpfs][s3][refresh]") {
	SECTION("httplib") {
		RunRefreshPublicationScenario("httplib", true, false);
	}
	SECTION("curl") {
		RunRefreshPublicationScenario("curl", true, false);
	}
}

TEST_CASE("S3 credential refresh composes with a concurrent region publication", "[httpfs][s3][refresh]") {
	SECTION("httplib") {
		RunRefreshPublicationScenario("httplib", false, true);
	}
	SECTION("curl") {
		RunRefreshPublicationScenario("curl", false, true);
	}
}

TEST_CASE("HTTPFS bulk delete follows a refreshed S3 endpoint", "[httpfs][s3][refresh]") {
	SECTION("httplib") {
		RunBulkDeleteEndpointRefreshScenario("httplib");
	}
	SECTION("curl") {
		RunBulkDeleteEndpointRefreshScenario("curl");
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

} // namespace duckdb
