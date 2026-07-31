#include "s3/s3_settings.hpp"

#include "s3/s3_auth.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

S3UploadConfig S3UploadConfig::ReadFrom(optional_ptr<FileOpener> opener) {
	uint64_t uploader_max_filesize;
	uint64_t max_parts_per_file;
	Value value;

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_filesize", value)) {
		uploader_max_filesize = DBConfig::ParseMemoryLimit(value.GetValue<string>());
	} else {
		uploader_max_filesize = S3UploadConfig::DEFAULT_MAX_FILESIZE;
	}
	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_parts_per_file", value)) {
		max_parts_per_file = value.GetValue<uint64_t>();
	} else {
		max_parts_per_file = S3UploadConfig::DEFAULT_MAX_PARTS_PER_FILE;
	}
	if (max_parts_per_file == 0) {
		throw InvalidInputException("s3_uploader_max_parts_per_file must be greater than zero");
	}

	const idx_t aws_minimum_part_size = 5242880;
	auto required_part_size = uploader_max_filesize / max_parts_per_file;
	auto minimum_part_size = MaxValue<uint64_t>(aws_minimum_part_size, required_part_size);
	auto part_size = ((minimum_part_size + Storage::DEFAULT_BLOCK_SIZE - 1) / Storage::DEFAULT_BLOCK_SIZE) *
	                 Storage::DEFAULT_BLOCK_SIZE;
	D_ASSERT(part_size * max_parts_per_file >= uploader_max_filesize);
	return {NumericCast<idx_t>(part_size), NumericCast<idx_t>(max_parts_per_file)};
}

void S3Settings::Register(DBConfig &config) {
	config.AddExtensionOption("s3_region", "S3 Region", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_access_key_id", "S3 Access Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_secret_access_key", "S3 Access Key", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_session_token", "S3 Session Token", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_endpoint", "S3 Endpoint", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_url_style", "S3 URL style", LogicalType::VARCHAR, Value("vhost"));
	config.AddExtensionOption("s3_use_ssl", "S3 use SSL", LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("s3_kms_key_id", "S3 KMS Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_url_compatibility_mode", "Disable Globs and Query Parameters on S3 URLs",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("s3_requester_pays", "S3 use requester pays mode", LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption(
	    "s3_allow_recursive_globbing",
	    "Whether globs on S3-like storage are optimized with recursive strategy (alternative is listing)",
	    LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("s3_uploader_max_filesize", "S3 Uploader max filesize (between 50GB and 5TB)",
	                          LogicalType::VARCHAR, "800GB");
	config.AddExtensionOption("s3_uploader_max_parts_per_file", "S3 Uploader max parts per file (between 1 and 10000)",
	                          LogicalType::UBIGINT, Value::UBIGINT(10000));
	config.AddExtensionOption("s3_version_id_pinning", "Pin S3 reads to a specific object version for consistency",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("merge_http_secret_into_s3_request", "Merges HTTP secret parameters into S3 requests",
	                          LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("httpfs_enable_credential_refresh", "Enable credential refresh for HTTPFS S3 secrets",
	                          LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("enable_global_s3_configuration",
	                          "Automatically fetch AWS credentials from environment variables.", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
}

void S3Settings::Initialize(DBConfig &config) {
	auto provider = make_uniq<AWSEnvironmentCredentialsProvider>(config);
	provider->SetAll();
}

} // namespace duckdb
