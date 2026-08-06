#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct DBConfig;
class FileOpener;

struct S3UploadConfig {
public:
	static S3UploadConfig Create(uint64_t max_file_size, uint64_t max_parts);
	static S3UploadConfig ReadFrom(optional_ptr<FileOpener> opener);

	idx_t PartSize(idx_t reserved_parts) const;
	bool HasPartCapacity(idx_t reserved_parts) const;

public:
	//! Default upload limits
	static constexpr uint64_t DEFAULT_MAX_FILESIZE = 80000000000;
	static constexpr uint64_t DEFAULT_MAX_PARTS_PER_FILE = 10000;
	static constexpr idx_t MAX_MULTIPART_PARTS = 10000;
	static constexpr idx_t MIN_MULTIPART_PART_SIZE = 5ULL * 1024ULL * 1024ULL;
	static constexpr idx_t MAX_MULTIPART_PART_SIZE = 5ULL * 1024ULL * 1024ULL * 1024ULL;
	static constexpr idx_t PART_SIZE_TIERS = 11;

	uint64_t max_file_size = 0;
	idx_t max_parts = 0;
	idx_t initial_part_size = 0;
	idx_t growth_interval = 0;
};

struct S3Settings {
	static void Register(DBConfig &config);
	static void Initialize(DBConfig &config);
};

} // namespace duckdb
