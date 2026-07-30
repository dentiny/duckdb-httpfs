#include "s3/s3fs.hpp"

#include "s3/s3_multi_part_upload.hpp"
#include "s3/s3_url.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

S3FileHandle::S3FileHandle(FileSystem &fs, const OpenFileInfo &file, FileOpenFlags flags,
                           unique_ptr<HTTPParams> http_params_p, const S3AuthParams &auth_params_p,
                           const S3ConfigParams &config_params_p)
    : HTTPFileHandle(fs, file, flags, std::move(http_params_p)), config_params(config_params_p) {
	auto captured = request_session->Capture();
	auto request_params = captured.snapshot->CreateRequestParams();
	request_session->TryPublish(captured.snapshot,
	                            make_shared_ptr<S3RequestSnapshot>(*request_params, auth_params_p, file.path));
	auto_fallback_to_full_file_download = false;
	if (flags.OpenForReading() && flags.OpenForWriting()) {
		throw NotImplementedException("Cannot open an HTTP file for both reading and writing");
	} else if (flags.OpenForAppending()) {
		throw NotImplementedException("Cannot open an HTTP file for appending");
	}
	if (file.extended_info) {
		auto entry = file.extended_info->options.find("s3_region");
		if (entry != file.extended_info->options.end()) {
			SetRegion(entry->second.ToString());
		}
	}
	if (flags.OpenForWriting()) {
		multi_part_upload = make_shared_ptr<S3MultiPartUpload>(*this);
	}
}

HTTPReadConfig S3FileHandle::BuildReadConfig() const {
	auto result = HTTPFileHandle::BuildReadConfig();
	if (!request_session->Capture().snapshot->Params().s3_version_id_pinning) {
		return result;
	}
	auto version_id = GetVersionId();
	if (!version_id.empty()) {
		result.condition.type = HTTPReadConditionType::S3_VERSION_ID;
		result.condition.value = std::move(version_id);
	}
	return result;
}

shared_ptr<const HTTPRequestSnapshot> S3FileHandle::CreateRequestSnapshot(const HTTPFSParams &params) const {
	auto captured = request_session->Capture();
	auto &s3_snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
	return make_shared_ptr<S3RequestSnapshot>(params, s3_snapshot.auth_params, s3_snapshot.refresh_path,
	                                          s3_snapshot.client_context, s3_snapshot.credential_refresh_enabled,
	                                          s3_snapshot.region_redirected, s3_snapshot.credential_generation);
}

S3FileHandle::~S3FileHandle() {
	if (Exception::UncaughtException()) {
		// We are in an exception, don't do anything
		return;
	}

	try {
		Close();
	} catch (...) { // NOLINT
	}
}

void S3FileHandle::SetRegion(string region_p) {
	string previous_region;
	S3RequestExecutor::SetSessionRegion(*request_session, region_p, previous_region);
}

S3ConfigParams S3ConfigParams::ReadFrom(optional_ptr<FileOpener> opener) {
	uint64_t uploader_max_filesize;
	uint64_t max_parts_per_file;
	uint64_t max_upload_threads;
	Value value;

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_filesize", value)) {
		uploader_max_filesize = DBConfig::ParseMemoryLimit(value.GetValue<string>());
	} else {
		uploader_max_filesize = S3ConfigParams::DEFAULT_MAX_FILESIZE;
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_parts_per_file", value)) {
		max_parts_per_file = value.GetValue<uint64_t>();
	} else {
		max_parts_per_file = S3ConfigParams::DEFAULT_MAX_PARTS_PER_FILE; // AWS Default
	}

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_thread_limit", value)) {
		max_upload_threads = value.GetValue<uint64_t>();
	} else {
		max_upload_threads = S3ConfigParams::DEFAULT_MAX_UPLOAD_THREADS;
	}

	return {uploader_max_filesize, max_parts_per_file, max_upload_threads};
}

void S3FileHandle::Close() {
	FinalizeUpload();
}

void S3FileHandle::FinalizeUpload() {
	if (flags.OpenForWriting() && multi_part_upload) {
		multi_part_upload->Finalize();
	}
}

// Wrapper around the BufferManager::Allocate to that allows limiting the number of buffers that will be handed out
BufferHandle S3FileSystem::Allocate(idx_t part_size, uint16_t max_threads) {
	return buffer_manager.Allocate(MemoryTag::EXTENSION, part_size);
}

EncryptionUtil &S3FileSystem::GetEncryptionUtil() {
	auto &config = DBConfig::GetConfig(buffer_manager.GetDatabase());
	if (!config.encryption_util) {
		throw InternalException("HTTPFS encryption util has not been initialized");
	}
	return *config.encryption_util;
}

unique_ptr<HTTPFileHandle> S3FileSystem::CreateHandle(const OpenFileInfo &file, FileOpenFlags flags,
                                                      optional_ptr<FileOpener> opener) {
	FileOpenerInfo info = {file.path};
	S3AuthParams auth_params = S3AuthParams::ReadFrom(opener, info);

	// Scan the query string for any s3 authentication parameters
	auto parsed_s3_url = S3Url::Parse(file.path, auth_params);
	S3Url::ReadQueryParams(parsed_s3_url.query_param, auth_params);

	auto &http_util = HTTPFSUtil::GetHTTPUtil(opener);
	auto params = http_util.InitializeParameters(opener, info);

	return duckdb::make_uniq<S3FileHandle>(*this, file, flags, std::move(params), auth_params,
	                                       S3ConfigParams::ReadFrom(opener));
}

void S3FileHandle::InitializeFromCacheEntry(const HTTPMetadataCacheEntry &cache_entry) {
	HTTPFileHandle::InitializeFromCacheEntry(cache_entry);
	auto entry = cache_entry.properties.find("s3_region");
	if (entry != cache_entry.properties.end()) {
		SetRegion(entry->second);
	}
}

HTTPMetadataCacheEntry S3FileHandle::GetCacheEntry() const {
	auto result = HTTPFileHandle::GetCacheEntry();
	auto captured = request_session->Capture();
	auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
	if (!snapshot.auth_params.region.empty()) {
		result.properties["s3_region"] = snapshot.auth_params.region;
	}
	return result;
}

void S3FileHandle::Initialize(optional_ptr<FileOpener> opener) {
	auto context = FileOpener::TryGetClientContext(opener);
	auto refresh_enabled = S3RequestExecutor::CredentialRefreshEnabled(opener);
	{
		auto captured = request_session->Capture();
		auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
		auto request_params = snapshot.CreateRequestParams();
		weak_ptr<ClientContext> weak_context;
		if (context && refresh_enabled) {
			weak_context = context->shared_from_this();
		}
		request_session->TryPublish(captured.snapshot, make_shared_ptr<S3RequestSnapshot>(
		                                                   *request_params, snapshot.auth_params, snapshot.refresh_path,
		                                                   std::move(weak_context), refresh_enabled,
		                                                   snapshot.region_redirected, snapshot.credential_generation));
	}
	HTTPFileHandle::Initialize(opener);

	if (flags.OpenForWriting()) {
		auto aws_minimum_part_size = 5242880; // 5 MiB https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
		auto max_part_count = config_params.max_parts_per_file;
		auto required_part_size = config_params.max_file_size / max_part_count;
		auto minimum_part_size = MaxValue<idx_t>(aws_minimum_part_size, required_part_size);

		// Round part size up to multiple of Storage::DEFAULT_BLOCK_SIZE
		multi_part_upload->part_size =
		    ((minimum_part_size + Storage::DEFAULT_BLOCK_SIZE - 1) / Storage::DEFAULT_BLOCK_SIZE) *
		    Storage::DEFAULT_BLOCK_SIZE;
		D_ASSERT(multi_part_upload->part_size * max_part_count >= config_params.max_file_size);
	}
}

bool S3FileSystem::CanHandleFile(const string &fpath) {
	return !S3Url::TryGetPrefix(fpath).empty();
}

void S3FileSystem::FileSync(FileHandle &handle) {
	auto &s3fh = handle.Cast<S3FileHandle>();
	s3fh.FinalizeUpload();
}

void S3FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &s3fh = handle.Cast<S3FileHandle>();
	if (!s3fh.flags.OpenForWriting()) {
		throw InternalException("Write called on file not opened in write mode");
	}
	annotated_lock_guard<annotated_mutex> guard(s3fh.cursor_mutex);
	int64_t bytes_written = 0;

	while (bytes_written < nr_bytes) {
		auto curr_location = location + bytes_written;

		if (curr_location != s3fh.file_offset) {
			throw InternalException("Non-sequential write not supported!");
		}

		// Find buffer for writing
		auto part_size = s3fh.multi_part_upload->part_size;
		auto write_buffer_idx = curr_location / part_size;

		// Get write buffer, may block until buffer is available
		auto write_buffer = s3fh.multi_part_upload->GetBuffer(write_buffer_idx);

		// Writing to buffer
		auto idx_to_write = curr_location - write_buffer->buffer_start;
		auto bytes_to_write = MinValue<idx_t>(nr_bytes - bytes_written, part_size - idx_to_write);
		memcpy(write_buffer->Ptr() + idx_to_write, data_ptr_cast(buffer) + bytes_written, bytes_to_write);
		write_buffer->idx += bytes_to_write;

		// Flush to HTTP if full
		if (write_buffer->idx >= part_size) {
			s3fh.multi_part_upload->FlushBuffer(write_buffer);
		}
		s3fh.file_offset += bytes_to_write;
		s3fh.length += bytes_to_write;
		bytes_written += bytes_to_write;
	}

	DUCKDB_LOG_FILE_SYSTEM_WRITE(handle, bytes_written, s3fh.file_offset - bytes_written);
}

string S3FileSystem::GetName() const {
	return "S3FileSystem";
}

} // namespace duckdb
