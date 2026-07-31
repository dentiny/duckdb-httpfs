#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct DBConfig;
class FileOpener;

struct S3UploadConfig {
	static constexpr uint64_t DEFAULT_MAX_FILESIZE = 800000000000; // 800GB
	static constexpr uint64_t DEFAULT_MAX_PARTS_PER_FILE = 10000;  // AWS DEFAULT

	idx_t part_size;
	idx_t max_parts;

	static S3UploadConfig ReadFrom(optional_ptr<FileOpener> opener);
};

struct S3Settings {
	static void Register(DBConfig &config);
	static void Initialize(DBConfig &config);
};

} // namespace duckdb
