#include "catch.hpp"

#include "http_request_session.hpp"
#include "s3fs.hpp"

#include <functional>

namespace duckdb {

namespace {

struct ClientLifecycle {
	idx_t initialized = 0;
	idx_t closed = 0;
	idx_t destroyed = 0;
};

class TrackingHTTPClient : public HTTPClient {
public:
	TrackingHTTPClient(const string &base_url, ClientLifecycle &lifecycle_p)
	    : HTTPClient(base_url), lifecycle(lifecycle_p) {
	}

	~TrackingHTTPClient() override {
		lifecycle.destroyed++;
	}

	void Initialize(HTTPParams &) override {
	}
	unique_ptr<HTTPResponse> Get(GetRequestInfo &) override {
		return Success();
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &) override {
		return Success();
	}
	unique_ptr<HTTPResponse> Head(HeadRequestInfo &) override {
		return Success();
	}
	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &) override {
		return Success();
	}
	unique_ptr<HTTPResponse> Post(PostRequestInfo &) override {
		return Success();
	}
	unique_ptr<HTTPResponse> Options(OptionsRequestInfo &) override {
		return Success();
	}

private:
	static unique_ptr<HTTPResponse> Success() {
		return make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
	}

	ClientLifecycle &lifecycle;
};

class TrackingHTTPUtil : public HTTPFSUtil {
public:
	explicit TrackingHTTPUtil(ClientLifecycle &lifecycle_p) : lifecycle(lifecycle_p) {
	}

	unique_ptr<HTTPClient> InitializeClient(HTTPParams &, const string &proto_host_port) override {
		lifecycle.initialized++;
		if (on_initialize) {
			on_initialize();
		}
		return make_uniq<TrackingHTTPClient>(proto_host_port, lifecycle);
	}

	void CloseClient(unique_ptr<HTTPClient> &&client) override {
		lifecycle.closed++;
		if (on_close) {
			on_close();
		}
		client.reset();
	}

	HTTPClientReuseMode GetClientReuseMode() const override {
		return reuse_mode;
	}

	ClientLifecycle &lifecycle;
	HTTPClientReuseMode reuse_mode = HTTPClientReuseMode::SESSION_LOCAL;
	std::function<void()> on_initialize;
	std::function<void()> on_close;
};

static HTTPFSParams CreateParams(TrackingHTTPUtil &http_util) {
	HTTPFSParams result(http_util);
	result.client_reuse_mode = http_util.GetClientReuseMode();
	result.httpfs_util = http_util;
	return result;
}

} // namespace

TEST_CASE("HTTP request snapshots are immutable and checked", "[httpfs][request-session]") {
	ClientLifecycle lifecycle;
	TrackingHTTPUtil http_util(lifecycle);
	auto params = CreateParams(http_util);
	params.user_agent = "httpfs-session-test";
	params.extra_headers["X-Test"] = "first";

	auto initial_snapshot = make_shared_ptr<HTTPRequestSnapshot>(params);
	auto session = make_shared_ptr<HTTPRequestSession>(initial_snapshot);
	auto initial = session->Capture();

	params.extra_headers["X-Test"] = "second";
	auto replacement = make_shared_ptr<HTTPRequestSnapshot>(params);
	auto current = session->Publish(initial, replacement);

	HTTPHeaders initial_headers;
	initial.snapshot->AddConfiguredHeaders(initial_headers);
	REQUIRE(initial_headers.GetHeaderValue("User-Agent") == "httpfs-session-test");
	REQUIRE(initial_headers.GetHeaderValue("X-Test") == "first");

	HTTPHeaders current_headers;
	current.snapshot->AddConfiguredHeaders(current_headers);
	current.snapshot->AddConfiguredHeaders(current_headers);
	REQUIRE(current_headers.GetHeaderValue("User-Agent") == "httpfs-session-test");
	REQUIRE(current_headers.GetHeaderValue("X-Test") == "second");

	auto stale_replacement = make_shared_ptr<HTTPRequestSnapshot>(CreateParams(http_util));
	auto still_current = session->Publish(initial, stale_replacement);
	REQUIRE(still_current.snapshot == current.snapshot);

	session->InvalidateClients();
	auto stale_generation_replacement = make_shared_ptr<HTTPRequestSnapshot>(CreateParams(http_util));
	auto newer_generation = session->Publish(current, stale_generation_replacement);
	REQUIRE(newer_generation.snapshot == current.snapshot);
	REQUIRE(newer_generation.client_generation > current.client_generation);
	REQUIRE(current.snapshot->type == HTTPRequestSnapshotType::HTTP);
}

TEST_CASE("HTTP client leases obey snapshot generations", "[httpfs][request-session]") {
	ClientLifecycle lifecycle;
	TrackingHTTPUtil http_util(lifecycle);
	auto params = CreateParams(http_util);
	auto session = make_shared_ptr<HTTPRequestSession>(make_shared_ptr<HTTPRequestSnapshot>(params));
	auto session_ptr = session.get();

	http_util.on_initialize = [session_ptr]() {
		auto captured = session_ptr->Capture();
		REQUIRE(captured.snapshot);
	};
	http_util.on_close = [session_ptr]() {
		auto captured = session_ptr->Capture();
		REQUIRE(captured.snapshot);
	};

	{
		auto captured = session->Capture();
		auto request_params = captured.snapshot->CreateRequestParams();
		REQUIRE(&request_params->http_util == &http_util);
		auto lease = session->AcquireClient(captured, *request_params, "http://localhost");
		REQUIRE(lease.Client());
	}
	REQUIRE(lifecycle.initialized == 1);
	REQUIRE(lifecycle.closed == 0);
	REQUIRE(lifecycle.destroyed == 0);

	{
		auto captured = session->Capture();
		auto request_params = captured.snapshot->CreateRequestParams();
		auto lease = session->AcquireClient(captured, *request_params, "http://localhost");
		REQUIRE(lease.Client());
		REQUIRE(lifecycle.initialized == 1);
		lease.Invalidate();
	}
	REQUIRE(lifecycle.closed == 0);
	REQUIRE(lifecycle.destroyed == 1);

	{
		auto captured = session->Capture();
		auto request_params = captured.snapshot->CreateRequestParams();
		auto lease = session->AcquireClient(captured, *request_params, "http://localhost");
		REQUIRE(lease.Client());

		auto incompatible_params = CreateParams(http_util);
		incompatible_params.http_proxy = "proxy.invalid";
		auto replacement = make_shared_ptr<HTTPRequestSnapshot>(incompatible_params);
		session->Publish(captured, replacement);
	}
	REQUIRE(lifecycle.initialized == 2);
	REQUIRE(lifecycle.closed == 0);
	REQUIRE(lifecycle.destroyed == 2);

	{
		auto captured = session->Capture();
		auto request_params = captured.snapshot->CreateRequestParams();
		auto lease = session->AcquireClient(captured, *request_params, "http://localhost");
		REQUIRE(lease.Client());
	}
	REQUIRE(lifecycle.initialized == 3);
	REQUIRE(lifecycle.closed == 0);
	REQUIRE(lifecycle.destroyed == 2);

	session.reset();
	REQUIRE(lifecycle.closed == 1);
	REQUIRE(lifecycle.destroyed == 3);
}

TEST_CASE("HTTP client leases preserve backend reuse policy", "[httpfs][request-session]") {
	SECTION("shared clients return through the HTTP util") {
		ClientLifecycle lifecycle;
		TrackingHTTPUtil http_util(lifecycle);
		http_util.reuse_mode = HTTPClientReuseMode::SHARED;
		auto params = CreateParams(http_util);
		auto session = make_shared_ptr<HTTPRequestSession>(make_shared_ptr<HTTPRequestSnapshot>(params));

		auto captured = session->Capture();
		auto request_params = captured.snapshot->CreateRequestParams();
		{
			auto lease = session->AcquireClient(captured, *request_params, "http://localhost");
			REQUIRE(lease.Client());
		}
		REQUIRE(lifecycle.initialized == 1);
		REQUIRE(lifecycle.closed == 1);
		REQUIRE(lifecycle.destroyed == 1);
	}

	SECTION("client-free implementations do not initialize or close clients") {
		ClientLifecycle lifecycle;
		TrackingHTTPUtil http_util(lifecycle);
		http_util.reuse_mode = HTTPClientReuseMode::NONE;
		auto params = CreateParams(http_util);
		auto session = make_shared_ptr<HTTPRequestSession>(make_shared_ptr<HTTPRequestSnapshot>(params));

		auto captured = session->Capture();
		auto request_params = captured.snapshot->CreateRequestParams();
		{
			auto lease = session->AcquireClient(captured, *request_params, "http://localhost");
			REQUIRE_FALSE(lease.Client());
		}
		REQUIRE(lifecycle.initialized == 0);
		REQUIRE(lifecycle.closed == 0);
		REQUIRE(lifecycle.destroyed == 0);
	}
}

} // namespace duckdb
