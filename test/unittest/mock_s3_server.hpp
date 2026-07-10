#pragma once

#include "duckdb/common/common.hpp"

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

struct MockS3ServerConfig {
	string bucket = "refresh-bucket";
	string object_key = "object.bin";
	string object_data = "abcdefghijklmnopqrstuvwxyz0123456789";
	string stale_key_id = "STALE_KEY";
	string etag = "\"httpfs-refresh-test-etag\"";
	MockS3RefreshTarget refresh_target = MockS3RefreshTarget::HEAD;
	//! Number of multipart part uploads (PUT with partNumber) to fail with a 400 before succeeding
	idx_t transient_put_failures = 0;
	//! Number of object GETs to fail with a 400 before succeeding
	idx_t transient_get_failures = 0;
	//! Number of multipart-init POSTs (uploads=) to fail with a 400 before succeeding
	idx_t transient_post_failures = 0;
	//! Whether injected 400s carry S3's retryable RequestTimeout code or a generic (non-retryable) code
	bool failure_is_request_timeout = true;
};

struct MockS3RequestObservation {
	string method;
	string path;
	string target;
	string range;
	string key_id;
	int status = 0;
};

class MockS3Server {
public:
	explicit MockS3Server(MockS3ServerConfig config);
	~MockS3Server();

	MockS3Server(const MockS3Server &) = delete;
	MockS3Server &operator=(const MockS3Server &) = delete;

	string Endpoint() const;
	string S3Path() const;
	const string &ObjectData() const;
	vector<MockS3RequestObservation> Observations() const;

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
