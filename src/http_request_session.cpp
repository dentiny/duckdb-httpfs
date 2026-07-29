#include "http_request_session.hpp"

#include "http_state.hpp"

namespace duckdb {

HTTPTransportSnapshot::HTTPTransportSnapshot(const HTTPFSParams &params)
    : http_util(params.http_util), httpfs_util(params.httpfs_util), timeout(params.timeout),
      timeout_usec(params.timeout_usec), retries(params.retries), retry_wait_ms(params.retry_wait_ms),
      retry_backoff(params.retry_backoff), keep_alive(params.keep_alive), follow_location(params.follow_location),
      override_verify_ssl(params.override_verify_ssl), verify_ssl(params.verify_ssl), http_proxy(params.http_proxy),
      http_proxy_port(params.http_proxy.empty() ? 0 : params.http_proxy_port),
      http_proxy_username(params.http_proxy_username), http_proxy_password(params.http_proxy_password),
      enable_server_cert_verification(params.enable_server_cert_verification),
      enable_curl_server_cert_verification(params.enable_curl_server_cert_verification),
      ca_cert_file(params.ca_cert_file), bearer_token(params.bearer_token), client_reuse(params.client_reuse_mode) {
}

bool HTTPTransportSnapshot::ClientCompatibleWith(const HTTPTransportSnapshot &other) const {
	return &http_util.get() == &other.http_util.get() && timeout == other.timeout &&
	       timeout_usec == other.timeout_usec && keep_alive == other.keep_alive &&
	       follow_location == other.follow_location && override_verify_ssl == other.override_verify_ssl &&
	       verify_ssl == other.verify_ssl && http_proxy == other.http_proxy &&
	       http_proxy_port == other.http_proxy_port && http_proxy_username == other.http_proxy_username &&
	       http_proxy_password == other.http_proxy_password &&
	       enable_server_cert_verification == other.enable_server_cert_verification &&
	       enable_curl_server_cert_verification == other.enable_curl_server_cert_verification &&
	       ca_cert_file == other.ca_cert_file && bearer_token == other.bearer_token &&
	       client_reuse == other.client_reuse;
}

HTTPRequestSnapshot::HTTPRequestSnapshot(const HTTPFSParams &params, HTTPRequestSnapshotType type_p)
    : type(type_p), transport(params), user_agent(params.user_agent), extra_headers(params.extra_headers),
      state(params.state), logger(params.logger), force_download(params.force_download),
      auto_fallback_to_full_download(params.auto_fallback_to_full_download),
      unsafe_disable_etag_checks(params.unsafe_disable_etag_checks),
      s3_version_id_pinning(params.s3_version_id_pinning), force_download_threshold(params.force_download_threshold),
      hf_max_per_page(params.hf_max_per_page) {
}

HTTPRequestSnapshot::~HTTPRequestSnapshot() = default;

bool HTTPRequestSnapshot::ClientCompatibleWith(const HTTPRequestSnapshot &other) const {
	return type == other.type && transport.ClientCompatibleWith(other.transport);
}

unique_ptr<HTTPFSParams> HTTPRequestSnapshot::CreateRequestParams() const {
	auto result = make_uniq<HTTPFSParams>(transport.http_util.get());
	result->timeout = transport.timeout;
	result->timeout_usec = transport.timeout_usec;
	result->retries = transport.retries;
	result->retry_wait_ms = transport.retry_wait_ms;
	result->retry_backoff = transport.retry_backoff;
	result->keep_alive = transport.keep_alive;
	result->follow_location = transport.follow_location;
	result->override_verify_ssl = transport.override_verify_ssl;
	result->verify_ssl = transport.verify_ssl;
	result->http_proxy = transport.http_proxy;
	result->http_proxy_port = transport.http_proxy.empty() ? 0 : transport.http_proxy_port;
	result->http_proxy_username = transport.http_proxy_username;
	result->http_proxy_password = transport.http_proxy_password;
	result->enable_server_cert_verification = transport.enable_server_cert_verification;
	result->enable_curl_server_cert_verification = transport.enable_curl_server_cert_verification;
	result->ca_cert_file = transport.ca_cert_file;
	result->bearer_token = transport.bearer_token;
	result->state = state;
	result->logger = logger;
	result->force_download = force_download;
	result->auto_fallback_to_full_download = auto_fallback_to_full_download;
	result->unsafe_disable_etag_checks = unsafe_disable_etag_checks;
	result->s3_version_id_pinning = s3_version_id_pinning;
	result->force_download_threshold = force_download_threshold;
	result->client_reuse_mode = transport.client_reuse;
	result->httpfs_util = transport.httpfs_util;
	result->hf_max_per_page = hf_max_per_page;
	result->user_agent = user_agent;
	result->extra_headers = extra_headers;
	result->pre_merged_headers = true;
	return result;
}

void HTTPRequestSnapshot::AddConfiguredHeaders(HTTPHeaders &headers) const {
	if (!user_agent.empty()) {
		headers.Insert("User-Agent", user_agent);
	}
	for (const auto &header : extra_headers) {
		headers[header.first] = header.second;
	}
}

HTTPClientLease::HTTPClientLease(shared_ptr<HTTPRequestSession> session_p, reference<HTTPUtil> http_util_p,
                                 HTTPClientReuseMode reuse_mode_p, idx_t generation_p, unique_ptr<HTTPClient> client_p)
    : session(std::move(session_p)), http_util(http_util_p), reuse_mode(reuse_mode_p), generation(generation_p),
      client(std::move(client_p)), reusable(true) {
}

HTTPClientLease::HTTPClientLease(HTTPClientLease &&other) noexcept
    : session(std::move(other.session)), http_util(other.http_util), reuse_mode(other.reuse_mode),
      generation(other.generation), client(std::move(other.client)), reusable(other.reusable) {
	other.reusable = false;
}

HTTPClientLease &HTTPClientLease::operator=(HTTPClientLease &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	Release();
	session = std::move(other.session);
	http_util = other.http_util;
	reuse_mode = other.reuse_mode;
	generation = other.generation;
	client = std::move(other.client);
	reusable = other.reusable;
	other.reusable = false;
	return *this;
}

HTTPClientLease::~HTTPClientLease() noexcept {
	Release();
}

void HTTPClientLease::Release() noexcept {
	if (!session) {
		return;
	}
	auto session_ref = std::move(session);
	session_ref->ReturnClient(std::move(client), http_util, reuse_mode, generation, reusable);
}

HTTPRequestSession::HTTPRequestSession(shared_ptr<const HTTPRequestSnapshot> snapshot_p)
    : current_snapshot(std::move(snapshot_p)) {
	D_ASSERT(current_snapshot);
}

HTTPRequestSession::~HTTPRequestSession() {
	vector<IdleClient> clients;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		clients = std::move(idle_clients);
	}
	CloseClients(std::move(clients));
}

CapturedHTTPRequestSnapshot HTTPRequestSession::Capture() const {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return {current_snapshot, client_generation};
}

CapturedHTTPRequestSnapshot HTTPRequestSession::Publish(const CapturedHTTPRequestSnapshot &failed,
                                                        shared_ptr<const HTTPRequestSnapshot> replacement) {
	D_ASSERT(replacement);
	vector<IdleClient> clients;
	CapturedHTTPRequestSnapshot result;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (current_snapshot != failed.snapshot || client_generation != failed.client_generation) {
			return {current_snapshot, client_generation};
		}
		if (!current_snapshot->ClientCompatibleWith(*replacement)) {
			client_generation++;
			clients = std::move(idle_clients);
		}
		current_snapshot = std::move(replacement);
		result = {current_snapshot, client_generation};
	}
	return result;
}

HTTPClientLease HTTPRequestSession::AcquireClient(const CapturedHTTPRequestSnapshot &captured,
                                                  HTTPFSParams &request_params, const string &proto_host_port) {
	auto reuse_mode = captured.snapshot->transport.client_reuse;
	unique_ptr<HTTPClient> client;
	if (reuse_mode == HTTPClientReuseMode::SESSION_LOCAL) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (captured.client_generation == client_generation) {
			for (idx_t i = idle_clients.size(); i > 0; i--) {
				auto &entry = idle_clients[i - 1];
				if (&entry.http_util.get() == &captured.snapshot->transport.http_util.get() && entry.client &&
				    entry.client->GetBaseUrl() == proto_host_port) {
					client = std::move(entry.client);
					idle_clients.erase_at(i - 1);
					break;
				}
			}
		}
	}
	if (!client && reuse_mode != HTTPClientReuseMode::NONE) {
		client = captured.snapshot->transport.http_util.get().InitializeClient(request_params, proto_host_port);
	}
	return HTTPClientLease(shared_from_this(), captured.snapshot->transport.http_util, reuse_mode,
	                       captured.client_generation, std::move(client));
}

void HTTPRequestSession::InvalidateClients() {
	vector<IdleClient> clients;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		client_generation++;
		clients = std::move(idle_clients);
	}
}

void HTTPRequestSession::ReturnClient(unique_ptr<HTTPClient> client, reference<HTTPUtil> http_util,
                                      HTTPClientReuseMode reuse_mode, idx_t generation, bool reusable) noexcept {
	if (!client) {
		return;
	}
	if (!reusable) {
		return;
	}
	if (reuse_mode == HTTPClientReuseMode::SESSION_LOCAL) {
		bool generation_current = false;
		try {
			annotated_lock_guard<annotated_mutex> guard(lock);
			generation_current = generation == client_generation;
			if (generation_current) {
				idle_clients.emplace_back(std::move(client), http_util);
				return;
			}
		} catch (...) { // NOLINT
			generation_current = true;
		}
		if (!generation_current) {
			return;
		}
	}
	try {
		http_util.get().CloseClient(std::move(client));
	} catch (...) { // NOLINT
	}
}

void HTTPRequestSession::CloseClients(vector<IdleClient> clients) noexcept {
	for (auto &entry : clients) {
		try {
			entry.http_util.get().CloseClient(std::move(entry.client));
		} catch (...) { // NOLINT
		}
	}
}

} // namespace duckdb
