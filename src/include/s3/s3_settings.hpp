#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct DBConfig;
class FileOpener;

struct S3UploadConfig {
	static constexpr uint64_t DEFAULT_MAX_FILESIZE = 80000000000; // 80GB
	static constexpr uint64_t DEFAULT_MAX_PARTS_PER_FILE = 10000; // AWS DEFAULT
	static constexpr idx_t MIN_MULTIPART_PART_SIZE = 5ULL * 1024ULL * 1024ULL;
	static constexpr idx_t MAX_MULTIPART_PART_SIZE = 5ULL * 1024ULL * 1024ULL * 1024ULL;

	idx_t aggregation_threshold;
	idx_t max_parts;

	static S3UploadConfig ReadFrom(optional_ptr<FileOpener> opener);
};

struct S3Settings {
	static void Register(DBConfig &config);
	static void Initialize(DBConfig &config);
};

} // namespace duckdb
