#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "httpfs_client.hpp"

namespace duckdb {

class HTTPRequestSession;
class HTTPState;
class Logger;

struct HTTPTransportSnapshot {
	explicit HTTPTransportSnapshot(const HTTPFSParams &params);

	reference<HTTPUtil> http_util;
	optional_ptr<HTTPFSUtil> httpfs_util;
	uint64_t timeout;
	uint64_t timeout_usec;
	uint64_t retries;
	uint64_t retry_wait_ms;
	float retry_backoff;
	bool keep_alive;
	bool follow_location;
	bool override_verify_ssl;
	bool verify_ssl;
	string http_proxy;
	idx_t http_proxy_port;
	string http_proxy_username;
	string http_proxy_password;
	bool enable_server_cert_verification;
	bool enable_curl_server_cert_verification;
	string ca_cert_file;
	string bearer_token;
	HTTPClientReuseMode client_reuse;

	bool ClientCompatibleWith(const HTTPTransportSnapshot &other) const;
};

enum class HTTPRequestSnapshotType : uint8_t { HTTP, S3 };

struct HTTPRequestSnapshot {
	static constexpr HTTPRequestSnapshotType TYPE = HTTPRequestSnapshotType::HTTP;

	explicit HTTPRequestSnapshot(const HTTPFSParams &params,
	                             HTTPRequestSnapshotType type_p = HTTPRequestSnapshotType::HTTP);
	virtual ~HTTPRequestSnapshot();

	const HTTPRequestSnapshotType type;
	HTTPTransportSnapshot transport;
	string user_agent;
	unordered_map<string, string> extra_headers;
	shared_ptr<HTTPState> state;
	shared_ptr<Logger> logger;
	bool force_download;
	bool auto_fallback_to_full_download;
	bool unsafe_disable_etag_checks;
	bool s3_version_id_pinning;
	idx_t force_download_threshold;
	idx_t hf_max_per_page;

	virtual bool ClientCompatibleWith(const HTTPRequestSnapshot &other) const;
	unique_ptr<HTTPFSParams> CreateRequestParams() const;
	void AddConfiguredHeaders(HTTPHeaders &headers) const;

	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast HTTP request snapshot - snapshot type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast HTTP request snapshot - snapshot type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CapturedHTTPRequestSnapshot {
	shared_ptr<const HTTPRequestSnapshot> snapshot;
	idx_t client_generation;
};

class HTTPClientLease {
	friend class HTTPRequestSession;

public:
	HTTPClientLease(HTTPClientLease &&other) noexcept;
	HTTPClientLease &operator=(HTTPClientLease &&other) noexcept;
	HTTPClientLease(const HTTPClientLease &) = delete;
	HTTPClientLease &operator=(const HTTPClientLease &) = delete;
	~HTTPClientLease() noexcept;

	unique_ptr<HTTPClient> &Client() {
		return client;
	}
	void Invalidate() {
		reusable = false;
	}

private:
	HTTPClientLease(shared_ptr<HTTPRequestSession> session_p, reference<HTTPUtil> http_util_p,
	                HTTPClientReuseMode reuse_mode_p, idx_t generation_p, unique_ptr<HTTPClient> client_p);
	void Release() noexcept;

	shared_ptr<HTTPRequestSession> session;
	reference<HTTPUtil> http_util;
	HTTPClientReuseMode reuse_mode;
	idx_t generation;
	unique_ptr<HTTPClient> client;
	bool reusable;
};

class HTTPRequestSession : public enable_shared_from_this<HTTPRequestSession> {
	friend class HTTPClientLease;

public:
	explicit HTTPRequestSession(shared_ptr<const HTTPRequestSnapshot> snapshot_p);
	~HTTPRequestSession();

	CapturedHTTPRequestSnapshot Capture() const DUCKDB_EXCLUDES(lock);
	CapturedHTTPRequestSnapshot Publish(const CapturedHTTPRequestSnapshot &failed,
	                                    shared_ptr<const HTTPRequestSnapshot> replacement) DUCKDB_EXCLUDES(lock);
	HTTPClientLease AcquireClient(const CapturedHTTPRequestSnapshot &captured, HTTPFSParams &request_params,
	                              const string &proto_host_port) DUCKDB_EXCLUDES(lock);
	void InvalidateClients() DUCKDB_EXCLUDES(lock);

private:
	struct IdleClient {
		IdleClient(unique_ptr<HTTPClient> client_p, reference<HTTPUtil> http_util_p)
		    : client(std::move(client_p)), http_util(http_util_p) {
		}

		unique_ptr<HTTPClient> client;
		reference<HTTPUtil> http_util;
	};

	void ReturnClient(unique_ptr<HTTPClient> client, reference<HTTPUtil> http_util, HTTPClientReuseMode reuse_mode,
	                  idx_t generation, bool reusable) noexcept DUCKDB_EXCLUDES(lock);
	static void CloseClients(vector<IdleClient> clients) noexcept;

	mutable annotated_mutex lock;
	shared_ptr<const HTTPRequestSnapshot> current_snapshot DUCKDB_GUARDED_BY(lock);
	idx_t client_generation DUCKDB_GUARDED_BY(lock) = 0;
	vector<IdleClient> idle_clients DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
