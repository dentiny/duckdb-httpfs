#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "http/http_state.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/client_data.hpp"
#include "http/http_metadata_cache.hpp"
#include "http/httpfs_client.hpp"
#include "http/http_request_session.hpp"

#include <functional>

namespace duckdb {

enum class HTTPReadConditionType : uint8_t { NONE, ETAG, S3_VERSION_ID };

struct HTTPReadCondition {
	HTTPReadConditionType type = HTTPReadConditionType::NONE;
	string value;
};

struct HTTPReadConfig {
	HTTPReadCondition condition;
	string etag;
	bool validate_etag = false;
	bool auto_fallback_to_full_download = false;
};

class RangeRequestNotSupportedException {
public:
	// Call static Throw instead: if thrown as exception DuckDB can't catch it.
	explicit RangeRequestNotSupportedException() = delete;

	static constexpr ExceptionType TYPE = ExceptionType::HTTP;
	static constexpr const char *MESSAGE =
	    "Content-Length from server mismatches requested range, server may not support range requests. You can try to "
	    "resolve this by enabling `SET force_download=true`";

	static void Throw() {
		throw HTTPException(MESSAGE);
	}
};

class HTTPFileSystem;
class S3FileSystem;

class HTTPFileHandle : public FileHandle {
	friend class HTTPFileSystem;
	friend class S3FileSystem;

public:
	HTTPFileHandle(FileSystem &fs, const OpenFileInfo &file, FileOpenFlags flags, unique_ptr<HTTPParams> params);
	~HTTPFileHandle() override;
	// This two-phase construction allows subclasses more flexible setup.
	virtual void Initialize(optional_ptr<FileOpener> opener);

	shared_ptr<HTTPRequestSession> request_session;

	// File handle info
	FileOpenFlags flags;
	idx_t length;
	timestamp_t last_modified;
	string etag;
	bool force_full_download;
	bool initialized = false;

	bool auto_fallback_to_full_file_download = true;

	// In write overwrite mode, we are not interested in the current state of the file: we're overwriting it.
	bool write_overwrite_mode = false;

	// Per-path download and range-support state shared by all handles in this query
	shared_ptr<HTTPFileState> file_state;
	optional_ptr<Allocator> buffer_allocator;

	const HTTPReadConfig &GetReadConfig() const;
	string GetVersionId() const;
	void SetVersionId(string version_id);

	// Record a completed range request into the network throughput estimate (latency + bandwidth)
	void RecordNetworkSample(double total_seconds, idx_t bytes, bool sample_has_ttfb, double ttfb_seconds)
	    DUCKDB_EXCLUDES(network_estimator_lock);
	// Expose the measured network throughput estimate to the (parquet) prefetch cost model.
	bool TryGetNetworkThroughput(NetworkThroughputEstimate &result) DUCKDB_EXCLUDES(network_estimator_lock);

private:
	// Sequential read/write position
	mutable annotated_mutex cursor_mutex;
	idx_t file_offset DUCKDB_GUARDED_BY(cursor_mutex);

	string version_id;
	HTTPReadConfig read_config;
	bool read_config_initialized = false;

	mutable annotated_mutex network_estimator_lock;
	double tp_latency_seconds DUCKDB_GUARDED_BY(network_estimator_lock) = 0;
	double tp_bandwidth_bps DUCKDB_GUARDED_BY(network_estimator_lock) = 0;
	idx_t tp_sample_count DUCKDB_GUARDED_BY(network_estimator_lock) = 0;
	// Minimum payload size for a request to contribute a bandwidth sample
	constexpr static idx_t MIN_BANDWIDTH_SAMPLE_BYTES = 1 << 16; // 64 KiB

public:
	void Close() override {
	}

protected:
	virtual shared_ptr<const HTTPRequestSnapshot> CreateRequestSnapshot(const HTTPFSParams &params) const;
	virtual HTTPReadConfig BuildReadConfig() const;
	//! Perform a HEAD request to get the file info (if not yet loaded)
	void LoadFileInfo();
	//! TODO: make base function virtual?
	void TryAddLogger(FileOpener &opener);

	virtual void InitializeFromCacheEntry(const HTTPMetadataCacheEntry &cache_entry);
	virtual HTTPMetadataCacheEntry GetCacheEntry() const;

private:
	void FinalizeReadConfig();
};

class HTTPFileSystem : public FileSystem {
public:
	static bool TryParseLastModifiedTime(const string &timestamp, timestamp_t &result);

	//! FileSystem overrides.
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		if (path.find('*') != std::string::npos && opener) {
			Value setting_val;
			if (FileOpener::TryGetCurrentSetting(opener, "allow_asterisks_in_http_paths", setting_val) &&
			    !setting_val.GetValue<bool>()) {
				throw InvalidInputException("Globs (`*`) for generic HTTP file is are not supported.\nConsider `SET "
				                            "allow_asterisks_in_http_paths = true;` to allow this behaviour");
			}
		}
		return {path}; // FIXME
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void FileSync(FileHandle &handle) override;
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) override {
		return handle.Cast<HTTPFileHandle>().TryGetNetworkThroughput(result);
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		return false;
	}
	string GetName() const override {
		return "HTTPFileSystem";
	}
	string PathSeparator(const string &path) override {
		return "/";
	}
	static void Verify();

	optional_ptr<HTTPMetadataCache> GetGlobalCache();
	virtual HTTPException GetHTTPError(FileHandle &, const HTTPResponse &response, const string &url);

	//! HTTP request overrides.
	virtual unique_ptr<HTTPResponse> HeadRequest(FileHandle &handle, string url, HTTPHeaders header_map);
	// Get Request with range parameter that GETs exactly buffer_out_len bytes from the url
	virtual unique_ptr<HTTPResponse> GetRangeRequest(FileHandle &handle, string url, HTTPHeaders header_map,
	                                                 const HTTPReadConfig &read_config, idx_t file_offset,
	                                                 char *buffer_out, idx_t buffer_out_len);
	// Get Request without a range (i.e., downloads full file)
	virtual unique_ptr<HTTPResponse> GetRequest(FileHandle &handle, string url, HTTPHeaders header_map,
	                                            const HTTPReadConfig &read_config, CachedFileDownload &download);
	virtual unique_ptr<HTTPResponse> DeleteRequest(FileHandle &handle, string url, HTTPHeaders header_map);
	//! Fully download a file, or wait for an in-progress download of the same path
	unique_ptr<CachedFileHandle> FullDownload(HTTPFileHandle &handle, const HTTPReadConfig &read_config,
	                                          bool &should_write_cache);

protected:
	//! FileSystem extension points used by HTTP handle setup.
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override {
		return true;
	}
	virtual unique_ptr<HTTPFileHandle> CreateHandle(const OpenFileInfo &file, FileOpenFlags flags,
	                                                optional_ptr<FileOpener> opener);

	//! Internal read helpers.
	bool TryRangeRequest(FileHandle &handle, string url, HTTPHeaders header_map, const HTTPReadConfig &read_config,
	                     idx_t file_offset, char *buffer_out, idx_t buffer_out_len);
	bool ReadAt(FileHandle &handle, void *buffer, idx_t read_size, idx_t location, const HTTPReadConfig &read_config);
	void ReadAtWithFallback(FileHandle &handle, void *buffer, idx_t read_size, idx_t location,
	                        const HTTPReadConfig &read_config);

	//! Shared request runners used by subclasses that need custom request setup/retry behavior.
	using HTTPSendCallback = std::function<unique_ptr<HTTPResponse>(BaseRequest &)>;
	using HTTPErrorCallback = std::function<HTTPException(const HTTPResponse &)>;

	unique_ptr<HTTPResponse> RunHeadRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
	                                        HTTPSendCallback send_request);
	unique_ptr<HTTPResponse> RunDeleteRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
	                                          HTTPSendCallback send_request);
	unique_ptr<HTTPResponse> RunPostRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
	                                        string &result, char *buffer_in, idx_t buffer_in_len,
	                                        HTTPSendCallback send_request);
	unique_ptr<HTTPResponse> RunPutRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
	                                       char *buffer_in, idx_t buffer_in_len, const string &content_type,
	                                       HTTPSendCallback send_request);
	unique_ptr<HTTPResponse> RunGetRequest(HTTPFileHandle &handle, string url, HTTPHeaders header_map,
	                                       HTTPFSParams &http_params, const HTTPReadConfig &read_config,
	                                       CachedFileDownload &download, HTTPErrorCallback get_error,
	                                       HTTPSendCallback send_request);
	unique_ptr<HTTPResponse> RunGetRangeRequest(HTTPFileHandle &handle, string url, HTTPHeaders header_map,
	                                            HTTPFSParams &http_params, const HTTPReadConfig &read_config,
	                                            idx_t file_offset, char *buffer_out, idx_t buffer_out_len,
	                                            HTTPErrorCallback get_error, HTTPSendCallback send_request);

private:
	void ValidateResponseETag(HTTPFileHandle &handle, const HTTPReadConfig &read_config, const HTTPResponse &response);
	void ThrowIfReadConditionFailed(HTTPFileHandle &handle, const HTTPReadConfig &read_config,
	                                const HTTPResponse &response);
	void EraseGlobalCacheEntry(const string &path) DUCKDB_EXCLUDES(global_cache_lock);

	// Global cache
	mutable annotated_mutex global_cache_lock;
	unique_ptr<HTTPMetadataCache> global_metadata_cache DUCKDB_GUARDED_BY(global_cache_lock);
};

} // namespace duckdb
