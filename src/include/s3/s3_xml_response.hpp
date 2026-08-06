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
	string error_access_key_id;
};

struct S3XMLError {
	string code;
	string message;
	string access_key_id;
};

struct S3XMLResponseParser {
	static bool TryParse(const string &input, S3XMLResponse &response);
	static bool TryParseError(const string &input, S3XMLError &error);
};

} // namespace duckdb
