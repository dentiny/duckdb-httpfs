#pragma once

#include "s3/s3_request.hpp"

namespace duckdb {

struct AWSListObjectV2 {
	static string Request(EncryptionUtil &encryption_util, HTTPRequestSession &session, const string &path,
	                      const string &continuation_token, bool use_delimiter = false,
	                      optional_idx max_keys = optional_idx());
	static void ParseFileList(string &aws_response, vector<OpenFileInfo> &result);
	static vector<string> ParseCommonPrefix(string &aws_response);
	static string ParseContinuationToken(string &aws_response);
};

} // namespace duckdb
