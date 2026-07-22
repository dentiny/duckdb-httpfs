#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <condition_variable>

namespace duckdb {

class CachedFileHandle;
class CachedFileDownload;

enum class RangeRequestSupport : uint8_t { UNKNOWN, SUPPORTED, NOT_SUPPORTED };

class RangeRequestState {
public:
	class Guard {
	public:
		explicit Guard(RangeRequestState &state_p) : state(state_p), support(state.support.load()), lock() {
			if (support == RangeRequestSupport::UNKNOWN) {
				lock = annotated_unique_lock<annotated_mutex>(state.state_mutex);
				support = state.support.load();
				if (support != RangeRequestSupport::UNKNOWN) {
					lock.unlock();
				}
			}
		}

		RangeRequestSupport Support() const {
			return support;
		}
		void MarkSupported() {
			if (support == RangeRequestSupport::UNKNOWN) {
				state.support = RangeRequestSupport::SUPPORTED;
				support = RangeRequestSupport::SUPPORTED;
			}
		}
		void MarkNotSupported() {
			state.support = RangeRequestSupport::NOT_SUPPORTED;
			support = RangeRequestSupport::NOT_SUPPORTED;
		}

	private:
		RangeRequestState &state;
		RangeRequestSupport support;
		annotated_unique_lock<annotated_mutex> lock;
	};

	Guard Lock() {
		return Guard(*this);
	}

private:
	annotated_mutex state_mutex;
	atomic<RangeRequestSupport> support = {RangeRequestSupport::UNKNOWN};
};

//! Represents a file that is intended to be fully downloaded, then used in parallel by multiple threads
class CachedFile : public enable_shared_from_this<CachedFile> {
	friend class CachedFileHandle;
	friend class CachedFileDownload;

public:
	unique_ptr<CachedFileHandle> GetHandle();
	unique_ptr<CachedFileDownload> StartDownload(Allocator &allocator);

private:
	//! Protects the download state and cached data
	mutable annotated_mutex lock;
	//! Notifies handles waiting for an in-progress download
	mutable std::condition_variable download_complete DUCKDB_GUARDED_BY(lock);
	//! Cached Data
	AllocatedData data DUCKDB_GUARDED_BY(lock);
	//! Data capacity
	uint64_t capacity DUCKDB_GUARDED_BY(lock) = 0;
	//! Size of file
	idx_t size DUCKDB_GUARDED_BY(lock) = 0;
	//! Whether a download is currently populating the cache
	bool downloading DUCKDB_GUARDED_BY(lock) = false;
	//! When initialized is true, the cached data is immutable
	bool initialized DUCKDB_GUARDED_BY(lock) = false;
};

//! Handle to a CachedFile
class CachedFileHandle {
public:
	explicit CachedFileHandle(shared_ptr<CachedFile> file_p);

	bool Initialized() const;
	const char *GetData() const;
	//! Return the size of the initialized file
	idx_t GetSize() const;

private:
	shared_ptr<CachedFile> file;
};

//! Exclusive handle for populating an uninitialized CachedFile
class CachedFileDownload {
	friend class CachedFile;

public:
	~CachedFileDownload();

	//! Reserve capacity without changing the number of downloaded bytes
	void Reserve(idx_t capacity);
	//! Append downloaded bytes, growing the allocation as needed
	void Append(const_data_ptr_t data, idx_t length);
	//! Reset bytes written by a prior request attempt
	void Reset();
	//! Indicate the file is fully downloaded and safe for parallel reading
	void Finalize();

private:
	CachedFileDownload(shared_ptr<CachedFile> file_p, Allocator &allocator_p);
	void ReserveInternal(idx_t capacity) DUCKDB_REQUIRES(file->lock);
	void Abort();

	shared_ptr<CachedFile> file;
	Allocator &allocator;
	bool active = true;
};

//! Per-path state shared by all HTTP file handles in a query
class HTTPFileState {
public:
	HTTPFileState() : cached_file(make_shared_ptr<CachedFile>()) {
	}

	unique_ptr<CachedFileHandle> GetCachedFileHandle() {
		return cached_file->GetHandle();
	}
	unique_ptr<CachedFileDownload> StartCachedFileDownload(Allocator &allocator) {
		return cached_file->StartDownload(allocator);
	}
	RangeRequestState::Guard LockRangeRequestState() {
		return range_request_state.Lock();
	}

private:
	shared_ptr<CachedFile> cached_file;
	RangeRequestState range_request_state;
};

class HTTPState : public ClientContextState {
public:
	//! Reset all counters and per-path state
	void Reset();
	//! Get per-path state, creating it if needed
	shared_ptr<HTTPFileState> GetFileState(const string &path);
	//! Erase all state for a path
	void EraseFileState(const string &path);
	//! Helper functions to get the HTTP state
	static shared_ptr<HTTPState> TryGetState(ClientContext &context);
	static shared_ptr<HTTPState> TryGetState(optional_ptr<FileOpener> opener);

	bool IsEmpty() {
		return head_count == 0 && get_count == 0 && put_count == 0 && post_count == 0 && delete_count == 0 &&
		       total_bytes_received == 0 && total_bytes_sent == 0;
	}

	atomic<idx_t> head_count {0};
	atomic<idx_t> get_count {0};
	atomic<idx_t> put_count {0};
	atomic<idx_t> post_count {0};
	atomic<idx_t> delete_count {0};
	atomic<idx_t> total_bytes_received {0};
	atomic<idx_t> total_bytes_sent {0};

	//! Called by the ClientContext when the current query ends
	void QueryEnd(ClientContext &context) override {
		Reset();
	}
	void WriteProfilingInformation(std::ostream &ss) override;
	mutex &CredentialRefreshLock() {
		return credential_refresh_mutex;
	}

private:
	//! Serializes credential provider refreshes after auth failures.
	mutex credential_refresh_mutex;
	//! Protects the per-path state map
	annotated_mutex file_states_mutex;
	//! Per-path state shared by all file handles in this query
	unordered_map<string, shared_ptr<HTTPFileState>> file_states DUCKDB_GUARDED_BY(file_states_mutex);
};

} // namespace duckdb
