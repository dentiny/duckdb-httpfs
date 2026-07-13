#include "mock_s3_server.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "httplib.hpp"

#include <atomic>
#include <cstring>
#include <mutex>
#include <thread>

namespace httplib = duckdb_httplib;

namespace duckdb {

namespace {

static string ExtractCredentialKey(const string &authorization) {
	auto credential_pos = authorization.find("Credential=");
	if (credential_pos == string::npos) {
		return string();
	}
	credential_pos += strlen("Credential=");
	auto end_pos = authorization.find('/', credential_pos);
	if (end_pos == string::npos) {
		return authorization.substr(credential_pos);
	}
	return authorization.substr(credential_pos, end_pos - credential_pos);
}

static bool ParseRange(const string &range, idx_t object_size) {
	static constexpr const char *PREFIX = "bytes=";
	if (range.rfind(PREFIX, 0) != 0) {
		return false;
	}
	auto dash_pos = range.find('-', strlen(PREFIX));
	if (dash_pos == string::npos) {
		return false;
	}
	idx_t start = 0;
	idx_t end = 0;
	try {
		start = std::stoull(range.substr(strlen(PREFIX), dash_pos - strlen(PREFIX)));
		end = std::stoull(range.substr(dash_pos + 1));
	} catch (...) {
		return false;
	}
	return start <= end && end < object_size;
}

static string GetHeader(const httplib::Request &request, const string &header) {
	if (!request.has_header(header)) {
		return string();
	}
	return request.get_header_value(header);
}

} // namespace

struct MockS3Server::Impl {
	explicit Impl(MockS3ServerConfig config_p) : config(std::move(config_p)) {
		remaining_put_failures = config.transient_put_failures;
		remaining_get_failures = config.transient_get_failures;
		remaining_head_failures = config.transient_head_failures;
		remaining_delete_failures = config.transient_delete_failures;
		remaining_post_failures = config.transient_post_failures;
		remaining_complete_post_failures = config.transient_complete_post_failures;
		RegisterRoutes();
		port = server.bind_to_any_port("127.0.0.1");
		if (port <= 0) {
			throw IOException("Failed to bind mock S3 server");
		}
		server_thread = std::thread([this]() { server.listen_after_bind(); });
		server.wait_until_ready();
	}

	~Impl() {
		server.stop();
		if (server_thread.joinable()) {
			server_thread.join();
		}
	}

	string Endpoint() const {
		return StringUtil::Format("127.0.0.1:%d", port);
	}

	string S3Path() const {
		return StringUtil::Format("s3://%s/%s", config.bucket, config.object_key);
	}

	vector<MockS3RequestObservation> Observations() const {
		std::lock_guard<std::mutex> lock(observation_lock);
		return observations;
	}

	bool MatchesRefreshTarget(const httplib::Request &request) const {
		auto range = GetHeader(request, "Range");
		switch (config.refresh_target) {
		case MockS3RefreshTarget::HEAD:
			return request.method == "HEAD";
		case MockS3RefreshTarget::FULL_GET:
			return request.method == "GET" && range.empty();
		case MockS3RefreshTarget::RANGE_GET:
			return request.method == "GET" && !range.empty();
		case MockS3RefreshTarget::PUT:
			return request.method == "PUT";
		case MockS3RefreshTarget::MULTIPART_INITIATE_POST:
			return request.method == "POST" && request.target.find("uploads") != string::npos;
		case MockS3RefreshTarget::MULTIPART_COMPLETE_POST:
			return request.method == "POST" && request.target.find("uploadId") != string::npos;
		case MockS3RefreshTarget::BULK_DELETE_POST:
			return request.method == "POST" && request.target.find("delete") != string::npos;
		case MockS3RefreshTarget::DELETE_OBJECT:
			return request.method == "DELETE";
		case MockS3RefreshTarget::LIST_OBJECTS_GET:
			return request.method == "GET" && request.target.find("list-type=2") != string::npos;
		default:
			throw InternalException("Unknown refresh target");
		}
	}

	bool ShouldRejectStaleCredentials(const httplib::Request &request) const {
		return ExtractCredentialKey(GetHeader(request, "Authorization")) == config.stale_key_id &&
		       MatchesRefreshTarget(request);
	}

	void Record(const httplib::Request &request, int status) const {
		MockS3RequestObservation observation;
		observation.method = request.method;
		observation.path = request.path;
		observation.target = request.target;
		observation.range = GetHeader(request, "Range");
		observation.key_id = ExtractCredentialKey(GetHeader(request, "Authorization"));
		observation.status = status;

		std::lock_guard<std::mutex> lock(observation_lock);
		observations.push_back(std::move(observation));
	}

	void SetObjectHeaders(httplib::Response &response) const {
		response.set_header("Accept-Ranges", "bytes");
		response.set_header("Content-Length", std::to_string(config.object_data.size()));
		response.set_header("ETag", config.etag);
	}

	void SendForbidden(const httplib::Request &request, httplib::Response &response) const {
		response.status = 403;
		response.set_content("<Error><Code>AccessDenied</Code><Message>stale credentials</Message></Error>",
		                     "application/xml");
		Record(request, response.status);
	}

	void SendPutSuccess(const httplib::Request &request, httplib::Response &response) const {
		response.status = 200;
		response.set_header("ETag", "\"httpfs-refresh-test-upload-etag\"");
		Record(request, response.status);
	}

	void SendS3Error400(const httplib::Request &request, httplib::Response &response, bool request_timeout) const {
		response.status = 400;
		if (config.truncated_failure_body) {
			response.set_content("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>RequestTimeout",
			                     "application/xml");
		} else if (request_timeout) {
			response.set_content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			                     "<Error><Code>RequestTimeout</Code><Message>Your socket connection to the server "
			                     "was not read from or written to within the timeout period.</Message></Error>",
			                     "application/xml");
		} else {
			response.set_content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			                     "<Error><Code>InvalidRequest</Code><Message>malformed request</Message></Error>",
			                     "application/xml");
		}
		Record(request, response.status);
	}

	void SendMultipartPostSuccess(const httplib::Request &request, httplib::Response &response) const {
		response.status = 200;
		if (request.target.find("uploads") != string::npos) {
			response.set_content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			                     "<InitiateMultipartUploadResult><UploadId>refresh-test-upload-id</UploadId></"
			                     "InitiateMultipartUploadResult>",
			                     "application/xml");
		} else {
			response.set_content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
			                     "<CompleteMultipartUploadResult><ETag>\"httpfs-refresh-test-final-etag\"</ETag></"
			                     "CompleteMultipartUploadResult>",
			                     "application/xml");
		}
		Record(request, response.status);
	}

	void SendBulkDeleteSuccess(const httplib::Request &request, httplib::Response &response) const {
		response.status = 200;
		response.set_content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
		                     "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></DeleteResult>",
		                     "application/xml");
		Record(request, response.status);
	}

	void SendListObjectsSuccess(const httplib::Request &request, httplib::Response &response) const {
		response.status = 200;
		auto unquoted_etag = StringUtil::Replace(config.etag, "\"", "");
		response.set_content(StringUtil::Format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
		                                        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
		                                        "<Name>%s</Name>"
		                                        "<Prefix></Prefix>"
		                                        "<KeyCount>1</KeyCount>"
		                                        "<MaxKeys>1000</MaxKeys>"
		                                        "<IsTruncated>false</IsTruncated>"
		                                        "<Contents>"
		                                        "<Key>%s</Key>"
		                                        "<ETag>&quot;%s&quot;</ETag>"
		                                        "<Size>%llu</Size>"
		                                        "</Contents>"
		                                        "</ListBucketResult>",
		                                        config.bucket, config.object_key, unquoted_etag,
		                                        static_cast<unsigned long long>(config.object_data.size())),
		                     "application/xml");
		Record(request, response.status);
	}

	void RegisterRoutes() {
		const string path = StringUtil::Format("/%s/%s", config.bucket, config.object_key);
		const string bucket_path = StringUtil::Format("/%s", config.bucket);
		const string bucket_path_with_slash = bucket_path + "/";

		server.set_pre_routing_handler([this, path](const httplib::Request &request, httplib::Response &response) {
			if (request.method != "HEAD" || request.path != path) {
				return httplib::Server::HandlerResponse::Unhandled;
			}
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return httplib::Server::HandlerResponse::Handled;
			}
			if (remaining_head_failures.load() > 0) {
				remaining_head_failures--;
				SendS3Error400(request, response, config.failure_is_request_timeout);
				return httplib::Server::HandlerResponse::Handled;
			}
			response.status = 200;
			SetObjectHeaders(response);
			Record(request, response.status);
			return httplib::Server::HandlerResponse::Handled;
		});

		server.Get(path, [this](const httplib::Request &request, httplib::Response &response) {
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return;
			}
			if (remaining_get_failures.load() > 0) {
				remaining_get_failures--;
				SendS3Error400(request, response, config.failure_is_request_timeout);
				return;
			}

			auto range = GetHeader(request, "Range");
			if (range.empty()) {
				response.status = 200;
				response.set_header("ETag", config.etag);
				response.set_content(config.object_data, "application/octet-stream");
				Record(request, response.status);
				return;
			}

			if (!ParseRange(range, config.object_data.size())) {
				response.status = 416;
				Record(request, response.status);
				return;
			}

			response.status = 206;
			response.set_header("Accept-Ranges", "bytes");
			response.set_header("ETag", config.etag);
			response.set_content(config.object_data, "application/octet-stream");
			Record(request, response.status);
		});

		auto list_objects = [this](const httplib::Request &request, httplib::Response &response) {
			if (request.target.find("list-type=2") == string::npos) {
				response.status = 404;
				Record(request, response.status);
				return;
			}
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return;
			}
			SendListObjectsSuccess(request, response);
		};
		server.Get(bucket_path, list_objects);
		server.Get(bucket_path_with_slash, list_objects);

		server.Put(path, [this](const httplib::Request &request, httplib::Response &response) {
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return;
			}
			if (remaining_put_failures.load() > 0) {
				remaining_put_failures--;
				SendS3Error400(request, response, config.failure_is_request_timeout);
				return;
			}
			SendPutSuccess(request, response);
		});

		server.Post(path, [this](const httplib::Request &request, httplib::Response &response) {
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return;
			}
			if (request.target.find("uploads") != string::npos && remaining_post_failures.load() > 0) {
				remaining_post_failures--;
				SendS3Error400(request, response, config.failure_is_request_timeout);
				return;
			}
			if (request.target.find("uploadId") != string::npos && remaining_complete_post_failures.load() > 0) {
				remaining_complete_post_failures--;
				SendS3Error400(request, response, config.failure_is_request_timeout);
				return;
			}
			SendMultipartPostSuccess(request, response);
		});

		auto bulk_delete = [this](const httplib::Request &request, httplib::Response &response) {
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return;
			}
			SendBulkDeleteSuccess(request, response);
		};
		server.Post(bucket_path, bulk_delete);
		server.Post(bucket_path_with_slash, bulk_delete);

		server.Delete(path, [this](const httplib::Request &request, httplib::Response &response) {
			if (ShouldRejectStaleCredentials(request)) {
				SendForbidden(request, response);
				return;
			}
			if (remaining_delete_failures.load() > 0) {
				remaining_delete_failures--;
				SendS3Error400(request, response, config.failure_is_request_timeout);
				return;
			}
			response.status = 204;
			Record(request, response.status);
		});
	}

	MockS3ServerConfig config;
	httplib::Server server;
	std::thread server_thread;
	int port = 0;
	mutable std::atomic<idx_t> remaining_put_failures {0};
	mutable std::atomic<idx_t> remaining_get_failures {0};
	mutable std::atomic<idx_t> remaining_head_failures {0};
	mutable std::atomic<idx_t> remaining_delete_failures {0};
	mutable std::atomic<idx_t> remaining_post_failures {0};
	mutable std::atomic<idx_t> remaining_complete_post_failures {0};
	mutable std::mutex observation_lock;
	mutable vector<MockS3RequestObservation> observations;
};

MockS3Server::MockS3Server(MockS3ServerConfig config) : impl(make_uniq<Impl>(std::move(config))) {
}

MockS3Server::~MockS3Server() {
}

string MockS3Server::Endpoint() const {
	return impl->Endpoint();
}

string MockS3Server::S3Path() const {
	return impl->S3Path();
}

const string &MockS3Server::ObjectData() const {
	return impl->config.object_data;
}

vector<MockS3RequestObservation> MockS3Server::Observations() const {
	return impl->Observations();
}

string MockS3RefreshTargetName(MockS3RefreshTarget target) {
	switch (target) {
	case MockS3RefreshTarget::HEAD:
		return "HEAD";
	case MockS3RefreshTarget::FULL_GET:
		return "FULL_GET";
	case MockS3RefreshTarget::RANGE_GET:
		return "RANGE_GET";
	case MockS3RefreshTarget::PUT:
		return "PUT";
	case MockS3RefreshTarget::MULTIPART_INITIATE_POST:
		return "MULTIPART_INITIATE_POST";
	case MockS3RefreshTarget::MULTIPART_COMPLETE_POST:
		return "MULTIPART_COMPLETE_POST";
	case MockS3RefreshTarget::BULK_DELETE_POST:
		return "BULK_DELETE_POST";
	case MockS3RefreshTarget::DELETE_OBJECT:
		return "DELETE";
	case MockS3RefreshTarget::LIST_OBJECTS_GET:
		return "LIST_OBJECTS_GET";
	default:
		throw InternalException("Unknown refresh target");
	}
}

bool MockS3HasObservation(const vector<MockS3RequestObservation> &observations, const string &method,
                          const string &key_id, int status, const string &range, const string &target_contains) {
	for (auto &observation : observations) {
		if (observation.method == method && observation.key_id == key_id && observation.status == status &&
		    observation.range == range &&
		    (target_contains.empty() || observation.target.find(target_contains) != string::npos)) {
			return true;
		}
	}
	return false;
}

string MockS3DescribeObservations(const vector<MockS3RequestObservation> &observations) {
	string result;
	for (auto &observation : observations) {
		if (!result.empty()) {
			result += "\n";
		}
		result += StringUtil::Format("%s %s status=%d key=%s range=%s target=%s", observation.method, observation.path,
		                             observation.status, observation.key_id, observation.range, observation.target);
	}
	return result;
}

} // namespace duckdb
