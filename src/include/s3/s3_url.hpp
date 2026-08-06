#pragma once

#include "s3/s3_auth.hpp"

namespace duckdb {

struct ParsedS3Url {
	string http_proto;
	string prefix;
	string host;
	string bucket;
	string key;
	string path;
	string query_param;
	string trimmed_s3_url;

	string GetHTTPUrl(const string &http_query_string = "") const;
};

struct S3Url {
	static string Decode(const string &input);
	static string Encode(const string &input, bool encode_slash = false);
	static string TryGetPrefix(const string &url);
	static string GetPrefix(const string &url);
	static ParsedS3Url Parse(const string &url, const S3AuthParams &params);
	static ParsedS3Url Resolve(const string &url, S3AuthParams &params);
	static string GetDisplayUrl(const string &url, const S3AuthParams &params);

private:
	static unordered_map<string, string> ParseQueryParameters(const string &url_query_param);
	static void ReadQueryParams(const string &url_query_param, S3AuthParams &params);
};

} // namespace duckdb
