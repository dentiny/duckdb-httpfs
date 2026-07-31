#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "http/httpfs.hpp"
#include "s3/s3_request.hpp"
#include "s3/s3_settings.hpp"

#include <exception>
#include <unordered_map>

#undef RemoveDirectory

namespace duckdb {

class S3FileSystem;
class S3UploadSession;

enum class S3PostRequestMode : uint8_t { REPLAYABLE, NON_REPLAYABLE };

class S3FileHandle : public HTTPFileHandle {
	friend class S3FileSystem;

public:
	S3FileHandle(FileSystem &fs, const OpenFileInfo &file, FileOpenFlags flags, unique_ptr<HTTPParams> http_params_p,
	             const S3AuthParams &auth_params_p, const S3UploadConfig &upload_config);
	~S3FileHandle() override;

	void Close() override;
	void Initialize(optional_ptr<FileOpener> opener) override;
	void FinalizeUpload();

protected:
	shared_ptr<const HTTPRequestSnapshot> CreateRequestSnapshot(const HTTPFSParams &params) const override;
	HTTPReadConfig BuildReadConfig() const override;
	void InitializeFromCacheEntry(const HTTPMetadataCacheEntry &cache_entry) override;
	HTTPMetadataCacheEntry GetCacheEntry() const override;

private:
	void SetRegion(string region_p);

private:
	unique_ptr<S3UploadSession> upload_session;
};

class S3FileSystem : public HTTPFileSystem {
public:
	explicit S3FileSystem(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
	}

	BufferManager &buffer_manager;

	//! FileSystem overrides.
	string GetName() const override;
	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void FileSync(FileHandle &handle) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	//! S3 is object storage so directories effectively always exist
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return true;
	}

	//! HTTP request overrides.
	unique_ptr<HTTPResponse> HeadRequest(FileHandle &handle, string s3_url, HTTPHeaders header_map) override;
	unique_ptr<HTTPResponse> GetRequest(FileHandle &handle, string url, HTTPHeaders header_map,
	                                    const HTTPReadConfig &read_config, CachedFileDownload &download) override;
	unique_ptr<HTTPResponse> GetRangeRequest(FileHandle &handle, string s3_url, HTTPHeaders header_map,
	                                         const HTTPReadConfig &read_config, idx_t file_offset,
	                                         data_ptr_t buffer_out, idx_t buffer_out_len) override;
	unique_ptr<HTTPResponse> PostRequest(HTTPRequestSession &session, string s3_url, string &buffer_out,
	                                     const_data_ptr_t buffer_in, idx_t buffer_in_len, string http_params = "",
	                                     S3PostRequestMode mode = S3PostRequestMode::REPLAYABLE);
	unique_ptr<HTTPResponse> PutRequest(HTTPRequestSession &session, string s3_url, const_data_ptr_t buffer_in,
	                                    idx_t buffer_in_len, string http_params = "");
	unique_ptr<HTTPResponse> DeleteRequest(HTTPRequestSession &session, string s3_url, string http_params);
	unique_ptr<HTTPResponse> DeleteRequest(FileHandle &handle, string s3_url, HTTPHeaders header_map) override;
	HTTPException GetHTTPError(FileHandle &, const HTTPResponse &response, const string &url) override;

	EncryptionUtil &GetEncryptionUtil();

protected:
	//! FileSystem extension points for S3 open/list/glob.
	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override {
		return true;
	}
	unique_ptr<MultiFileList> GlobFilesExtended(const string &path, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override;
	bool SupportsGlobExtended() const override {
		return true;
	}
	unique_ptr<HTTPFileHandle> CreateHandle(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

private:
	unique_ptr<HTTPResponse> RunS3BulkDeleteRequest(HTTPRequestSession &session, const string &secret_lookup_url,
	                                                const string &body, string &result);
};
} // namespace duckdb
