#pragma once

#include "s3/s3_request.hpp"
#include "s3/s3_xml_response.hpp"

namespace duckdb {

struct AWSListObjectV2 {
	static S3ListObjectsV2Result Request(EncryptionUtil &encryption_util, HTTPRequestSession &session,
	                                     const string &path, const string &continuation_token,
	                                     bool use_delimiter = false, optional_idx max_keys = optional_idx());
	static void AppendFileList(const S3ListObjectsV2Result &response, vector<OpenFileInfo> &result);
};

} // namespace duckdb
