#include "catch.hpp"

#include "s3/s3_settings.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

TEST_CASE("S3 upload config uses adaptive part sizes", "[httpfs][s3][upload]") {
	SECTION("default schedule") {
		auto config =
		    S3UploadConfig::Create(S3UploadConfig::DEFAULT_MAX_FILESIZE, S3UploadConfig::DEFAULT_MAX_PARTS_PER_FILE);
		REQUIRE(config.max_file_size == S3UploadConfig::DEFAULT_MAX_FILESIZE);
		REQUIRE(config.max_parts == S3UploadConfig::DEFAULT_MAX_PARTS_PER_FILE);
		REQUIRE(config.initial_part_size == S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.growth_interval == 910);
		REQUIRE(config.PartSize(0) == S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(909) == S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(910) == 2 * S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(1819) == 2 * S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(1820) == 4 * S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(9100) == S3UploadConfig::MAX_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(9999) == S3UploadConfig::MAX_MULTIPART_PART_SIZE);
	}

	SECTION("small part budget") {
		auto config = S3UploadConfig::Create(16ULL * 1024ULL * 1024ULL, 11);
		REQUIRE(config.initial_part_size == S3UploadConfig::MIN_MULTIPART_PART_SIZE);
		REQUIRE(config.growth_interval == 1);
		REQUIRE(config.PartSize(0) == 5ULL * 1024ULL * 1024ULL);
		REQUIRE(config.PartSize(1) == 10ULL * 1024ULL * 1024ULL);
		REQUIRE(config.PartSize(10) == S3UploadConfig::MAX_MULTIPART_PART_SIZE);
		REQUIRE(config.HasPartCapacity(10));
		REQUIRE_FALSE(config.HasPartCapacity(11));
	}

	SECTION("initial size is block aligned") {
		auto minimum_config = S3UploadConfig::Create(16ULL * 1024ULL * 1024ULL, 11);
		uint64_t minimum_capacity = 0;
		for (idx_t part_index = 0; part_index < minimum_config.max_parts; part_index++) {
			minimum_capacity += minimum_config.PartSize(part_index);
		}
		auto config = S3UploadConfig::Create(minimum_capacity + 1, 11);
		REQUIRE(config.initial_part_size == S3UploadConfig::MIN_MULTIPART_PART_SIZE + Storage::DEFAULT_BLOCK_SIZE +
		                                        Storage::DEFAULT_BLOCK_HEADER_SIZE);
	}

	SECTION("maximum supported schedule") {
		const auto maximum_file_size =
		    NumericCast<uint64_t>(S3UploadConfig::MAX_MULTIPART_PART_SIZE) * S3UploadConfig::MAX_MULTIPART_PARTS;
		auto config = S3UploadConfig::Create(maximum_file_size, S3UploadConfig::MAX_MULTIPART_PARTS);
		REQUIRE(config.initial_part_size == S3UploadConfig::MAX_MULTIPART_PART_SIZE);
		REQUIRE(config.PartSize(0) == S3UploadConfig::MAX_MULTIPART_PART_SIZE);

		auto single_part = S3UploadConfig::Create(S3UploadConfig::MAX_MULTIPART_PART_SIZE, 1);
		REQUIRE(single_part.initial_part_size == S3UploadConfig::MAX_MULTIPART_PART_SIZE);
		REQUIRE(single_part.HasPartCapacity(0));
		REQUIRE_FALSE(single_part.HasPartCapacity(1));
	}

	SECTION("invalid limits") {
		const auto maximum_file_size =
		    NumericCast<uint64_t>(S3UploadConfig::MAX_MULTIPART_PART_SIZE) * S3UploadConfig::MAX_MULTIPART_PARTS;
		REQUIRE_THROWS(S3UploadConfig::Create(0, 1));
		REQUIRE_THROWS(S3UploadConfig::Create(1, 0));
		REQUIRE_THROWS(S3UploadConfig::Create(1, S3UploadConfig::MAX_MULTIPART_PARTS + 1));
		REQUIRE_THROWS(S3UploadConfig::Create(maximum_file_size + 1, S3UploadConfig::MAX_MULTIPART_PARTS));
		REQUIRE_THROWS(S3UploadConfig::Create(NumericLimits<uint64_t>::Maximum(), S3UploadConfig::MAX_MULTIPART_PARTS));
	}
}

} // namespace duckdb
