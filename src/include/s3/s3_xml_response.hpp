#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class S3XMLResponseType : uint8_t { UNKNOWN, MULTIPART_INITIALIZATION, MULTIPART_COMPLETION, ERROR };

struct S3XMLResponse {
	S3XMLResponseType type = S3XMLResponseType::UNKNOWN;
	string upload_id;
	string etag;
	string error_code;
	string error_message;
};

struct S3XMLResponseParser {
	static bool TryParse(const string &input, S3XMLResponse &response);
};

} // namespace duckdb
