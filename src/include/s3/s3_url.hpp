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

	string GetHTTPUrl(S3AuthParams &auth_params, const string &http_query_string = "");
};

struct S3Url {
	static string Decode(const string &input);
	static string Encode(const string &input, bool encode_slash = false);
	static bool IsGCS(const string &url);
	static string TryGetPrefix(const string &url);
	static string GetPrefix(const string &url);
	static ParsedS3Url Parse(const string &url, const S3AuthParams &params);
	static void ReadQueryParams(const string &url_query_param, S3AuthParams &params);
};

} // namespace duckdb
