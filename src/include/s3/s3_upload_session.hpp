#pragma once

#include "s3/s3_settings.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

#include <condition_variable>

namespace duckdb {

class HTTPRequestSession;
class S3FileSystem;

class S3UploadSession {
private:
	enum class FailureDisposition : uint8_t { DEFINITIVE, AMBIGUOUS };

public:
	class WriteClaim {
		friend class S3UploadSession;

	public:
		WriteClaim(WriteClaim &&other) noexcept;
		WriteClaim(const WriteClaim &) = delete;
		WriteClaim &operator=(const WriteClaim &) = delete;
		~WriteClaim();

		void Finish();

	private:
		explicit WriteClaim(S3UploadSession &session_p);
		[[noreturn]] void Fail(ErrorData error, FailureDisposition disposition);

	private:
		reference<S3UploadSession> session;
		bool finished = false;
	};

public:
	S3UploadSession(S3FileSystem &s3fs, shared_ptr<HTTPRequestSession> request_session, string path,
	                S3UploadConfig config);
	~S3UploadSession() noexcept;

	WriteClaim Write(const_data_ptr_t data, idx_t size, idx_t location);
	void Finalize();

private:
	enum class InitializationState : uint8_t { NOT_STARTED, IN_PROGRESS, SUCCEEDED, FAILED };
	enum class CleanupState : uint8_t { NONE, IN_PROGRESS, COMPLETE };

	struct FailureSnapshot {
		shared_ptr<const ErrorData> primary_error;
		shared_ptr<const ErrorData> abort_error;
	};

	struct BufferedPart {
		BufferedPart(BufferHandle buffer_p, idx_t capacity_p) : buffer(std::move(buffer_p)), capacity(capacity_p) {
		}

		data_ptr_t Ptr() {
			return buffer.GetDataMutable();
		}

		BufferHandle buffer;
		const idx_t capacity;
		idx_t size = 0;
	};

	struct MultipartSnapshot {
		shared_ptr<const string> upload_id;
		vector<string> etags;
	};

	struct PreparedPart {
		PreparedPart(idx_t part_number_p, const_data_ptr_t data_p, idx_t size_p)
		    : part_number(part_number_p), data(data_p), size(size_p) {
		}
		PreparedPart(idx_t part_number_p, unique_ptr<BufferedPart> buffered_part_p)
		    : part_number(part_number_p), buffered_part(std::move(buffered_part_p)), data(buffered_part->Ptr()),
		      size(buffered_part->size) {
		}

		idx_t part_number;
		unique_ptr<BufferedPart> buffered_part;
		const_data_ptr_t data;
		idx_t size;
	};

	struct PreparedWrite {
		vector<PreparedPart> parts;
		unique_ptr<BufferedPart> buffered_part;
	};

private:
	void BeginWriteOperation() DUCKDB_EXCLUDES(state_lock);
	PreparedWrite PrepareWrite(const_data_ptr_t data, idx_t size, idx_t location) DUCKDB_EXCLUDES(state_lock);
	unique_ptr<BufferedPart> BeginFinalize(bool &already_finalized) DUCKDB_EXCLUDES(state_lock);
	void ReleaseOperation(bool rethrow_failure) DUCKDB_EXCLUDES(state_lock);
	void ReleaseWrite() DUCKDB_EXCLUDES(state_lock);
	void ReleaseWriteNoThrow() noexcept DUCKDB_EXCLUDES(state_lock);
	void FinishFinalize() DUCKDB_EXCLUDES(state_lock);
	void LatchFailure(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	void LatchFailureLocked(shared_ptr<const ErrorData> error, FailureDisposition disposition)
	    DUCKDB_REQUIRES(state_lock);
	[[noreturn]] void FailOperation(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	FailureSnapshot CaptureFailure() const DUCKDB_REQUIRES(state_lock);
	[[noreturn]] static void ThrowFailure(const FailureSnapshot &failure);
	shared_ptr<const ErrorData> AbortMultipartUpload(const string &upload_id);
	void ThrowIfFailed() DUCKDB_EXCLUDES(state_lock);

	unique_ptr<BufferedPart> AllocateBufferedPart(idx_t capacity);
	idx_t AppendToBufferedPart(BufferedPart &buffered_part, const_data_ptr_t data, idx_t size);
	void ReservePart(PreparedWrite &write, const_data_ptr_t data, idx_t size) DUCKDB_REQUIRES(state_lock);
	void ReservePart(PreparedWrite &write, unique_ptr<BufferedPart> buffered_part) DUCKDB_REQUIRES(state_lock);
	shared_ptr<const string> EnsureMultipartUpload();
	void PublishInitializationFailure(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	string InitializeMultipartUpload();
	string Upload(const_data_ptr_t data, idx_t size, const string &query_param);
	void UploadPart(PreparedPart &part);
	void UploadSingle(BufferedPart &buffered_part);
	void UploadEmpty();
	void StorePartETag(idx_t part_number, string etag) DUCKDB_EXCLUDES(state_lock);
	MultipartSnapshot GetMultipartSnapshot() DUCKDB_EXCLUDES(state_lock);
	void CompleteMultipartUpload();

private:
	reference<S3FileSystem> s3fs;
	shared_ptr<HTTPRequestSession> request_session;
	const string path;
	const string display_path;
	const S3UploadConfig config;

	annotated_mutex state_lock;
	std::condition_variable state_changed;
	bool finalized DUCKDB_GUARDED_BY(state_lock) = false;
	bool finalizing DUCKDB_GUARDED_BY(state_lock) = false;
	idx_t active_operations DUCKDB_GUARDED_BY(state_lock) = 0;
	idx_t next_offset DUCKDB_GUARDED_BY(state_lock) = 0;
	unique_ptr<BufferedPart> buffered_part DUCKDB_GUARDED_BY(state_lock);
	InitializationState initialization_state DUCKDB_GUARDED_BY(state_lock) = InitializationState::NOT_STARTED;
	shared_ptr<const string> multipart_upload_id DUCKDB_GUARDED_BY(state_lock);
	vector<string> part_etags DUCKDB_GUARDED_BY(state_lock);
	FailureSnapshot primary_failure DUCKDB_GUARDED_BY(state_lock);
	FailureDisposition failure_disposition DUCKDB_GUARDED_BY(state_lock) = FailureDisposition::DEFINITIVE;
	bool abort_suppressed DUCKDB_GUARDED_BY(state_lock) = false;
	CleanupState cleanup_state DUCKDB_GUARDED_BY(state_lock) = CleanupState::NONE;
};

} // namespace duckdb
