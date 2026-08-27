#pragma once

#include "s3/s3_auth.hpp"

namespace duckdb {

struct ParsedS3Url {
public:
	string GetHTTPUrl(const string &http_query_string = "") const;
	string GetBucketPath() const;

public:
	string http_proto;
	string prefix;
	string host;
	string bucket;
	string key;
	string path;
	string query_param;
	string trimmed_s3_url;
};

enum class S3URLEncodeMode : uint8_t { PATH, QUERY_COMPONENT };

struct S3Url {
	static string Decode(const string &input);
	static string Encode(const string &input, S3URLEncodeMode mode);
	static ParsedS3Url Parse(const string &url, const S3AuthParams &params);
	static ParsedS3Url Resolve(const string &url, S3AuthParams &params);
	static string GetDisplayUrl(const string &url, const S3AuthParams &params);

private:
	static unordered_map<string, string> ParseQueryParameters(const string &url_query_param);
	static void ReadQueryParams(const string &url_query_param, S3AuthParams &params);
};

} // namespace duckdb
