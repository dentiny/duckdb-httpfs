#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

enum class MockS3RefreshTarget : uint8_t {
	HEAD,
	FULL_GET,
	RANGE_GET,
	PUT,
	MULTIPART_INITIATE_POST,
	MULTIPART_COMPLETE_POST,
	BULK_DELETE_POST,
	DELETE_OBJECT,
	LIST_OBJECTS_GET
};

enum class MockS3RangeBehavior : uint8_t { NORMAL, IGNORE_RANGE, TRUNCATE_TRANSFER, SHORT_SUCCESS };

struct MockS3ServerConfig {
	string bucket = "refresh-bucket";
	string object_key = "object.bin";
	string object_data = "abcdefghijklmnopqrstuvwxyz0123456789";
	string stale_key_id = "STALE_KEY";
	int stale_auth_status = 403;
	string stale_auth_error_code = "AccessDenied";
	string etag = "\"httpfs-refresh-test-etag\"";
	//! Redirect signed requests to this region when their credential scope uses a different one
	string required_region;
	MockS3RefreshTarget refresh_target = MockS3RefreshTarget::HEAD;
	//! Answer this many leading ListObjectsV2 requests with HTTP 503 SlowDown
	idx_t transient_503_lists = 0;
	//! Answer this many leading ListObjectsV2 requests with HTTP 400
	idx_t transient_400_lists = 0;
	//! Number of object PUTs to fail with a 400 before succeeding
	idx_t transient_put_failures = 0;
	//! Number of object GETs to fail with a 400 before succeeding
	idx_t transient_get_failures = 0;
	//! Range response behavior to inject
	MockS3RangeBehavior range_behavior = MockS3RangeBehavior::NORMAL;
	//! Number of leading range GETs affected by transient range behaviors
	idx_t range_behavior_requests = 0;
	//! Number of bytes to omit from an injected truncated range response
	idx_t truncated_range_bytes = 1;
	//! Override the Content-Length reported by HEAD while keeping the GET body unchanged
	optional_idx head_content_length;
	//! Send successful full GETs with chunked transfer encoding and no Content-Length
	bool chunked_full_get = false;
	//! Advertise byte-range support on HEAD responses
	bool advertise_ranges = true;
	//! Hold the first range response body until a second range request arrives
	bool block_first_range_body_until_second_range = false;
	//! Hold a full GET response body until ReleaseFullGet is called
	bool block_full_get_until_released = false;
	//! Number of object HEADs to fail with a 400 before succeeding
	idx_t transient_head_failures = 0;
	//! Number of object HEADs to answer with 404 before succeeding
	idx_t head_not_found_requests = 0;
	//! Number of object DELETEs to fail with a 400 before succeeding
	idx_t transient_delete_failures = 0;
	//! Number of multipart-init POSTs (uploads=) to fail with a 400 before succeeding
	idx_t transient_post_failures = 0;
	//! Number of multipart-complete POSTs (uploadId=) to fail with a 400 before succeeding
	idx_t transient_complete_post_failures = 0;
	//! Whether injected 400s carry S3's retryable RequestTimeout code or a generic (non-retryable) code
	bool failure_is_request_timeout = true;
	//! Whether injected 400 bodies are truncated mid-XML (an open <Code> with no closing tag)
	bool truncated_failure_body = false;
};

struct MockS3RequestObservation {
	string method;
	string path;
	string target;
	string range;
	string key_id;
	string region;
	string user_agent;
	string session_header;
	idx_t user_agent_count = 0;
	idx_t session_header_count = 0;
	int status = 0;
	//! Client's ephemeral source port; a new connection shows a new port
	int remote_port = 0;
};

class MockS3Server {
public:
	explicit MockS3Server(MockS3ServerConfig config);
	~MockS3Server();

	MockS3Server(const MockS3Server &) = delete;
	MockS3Server &operator=(const MockS3Server &) = delete;

	string Endpoint() const;
	string HTTPPath() const;
	string S3Path() const;
	const string &ObjectData() const;
	vector<MockS3RequestObservation> Observations() const;
	bool WaitForFullGet();
	void ReleaseFullGet();

private:
	struct Impl;
	unique_ptr<Impl> impl;
};

string MockS3RefreshTargetName(MockS3RefreshTarget target);

bool MockS3HasObservation(const vector<MockS3RequestObservation> &observations, const string &method,
                          const string &key_id, int status, const string &range = string(),
                          const string &target_contains = string());
string MockS3DescribeObservations(const vector<MockS3RequestObservation> &observations);

} // namespace duckdb
