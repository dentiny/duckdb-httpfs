#pragma once

#include "s3/s3_settings.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

class HTTPRequestSession;
class S3FileSystem;

class S3UploadSession {
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
	enum class State : uint8_t { OPEN, MULTIPART_ACTIVE, FINALIZED, FAILED, AMBIGUOUS };
	enum class Operation : uint8_t { NONE, WRITE, FINALIZE };
	enum class FailureDisposition : uint8_t { DEFINITIVE, AMBIGUOUS };

	struct FailureSnapshot {
		shared_ptr<const ErrorData> primary_error;
		shared_ptr<const ErrorData> abort_error;
	};

	struct BufferedPart {
		explicit BufferedPart(BufferHandle buffer_p) : buffer(std::move(buffer_p)) {
		}

		data_ptr_t Ptr() {
			return buffer.GetDataMutable();
		}

		BufferHandle buffer;
		idx_t size = 0;
	};

	struct MultipartSnapshot {
		shared_ptr<const string> upload_id;
		vector<string> etags;
	};

private:
	unique_ptr<BufferedPart> BeginWrite(idx_t location, idx_t size) DUCKDB_EXCLUDES(state_lock);
	unique_ptr<BufferedPart> BeginFinalize(bool &already_finalized) DUCKDB_EXCLUDES(state_lock);
	void StoreWriteResult(unique_ptr<BufferedPart> buffered_part, idx_t size) DUCKDB_EXCLUDES(state_lock);
	void ReleaseWrite() DUCKDB_EXCLUDES(state_lock);
	void FinishFinalize() DUCKDB_EXCLUDES(state_lock);
	[[noreturn]] void FailOperation(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock);
	FailureSnapshot CaptureFailure() const DUCKDB_REQUIRES(state_lock);
	[[noreturn]] static void ThrowFailure(const FailureSnapshot &failure);
	shared_ptr<const ErrorData> AbortMultipartUpload(const string &upload_id);

	unique_ptr<BufferedPart> AllocateBufferedPart();
	idx_t AppendToBufferedPart(BufferedPart &buffered_part, const_data_ptr_t data, idx_t size);
	bool ShouldBufferUnbufferedSpan(idx_t size) DUCKDB_EXCLUDES(state_lock);
	void WriteUnbufferedSpan(unique_ptr<BufferedPart> &buffered_part, const_data_ptr_t data, idx_t size);
	void EnsureMultipartUpload();
	string InitializeMultipartUpload();
	string Upload(const_data_ptr_t data, idx_t size, const string &query_param);
	void UploadPart(const_data_ptr_t data, idx_t size);
	void UploadBufferedPart(BufferedPart &buffered_part);
	void UploadSingle(BufferedPart &buffered_part);
	void UploadEmpty();
	void StorePartETag(idx_t part_number, string etag) DUCKDB_EXCLUDES(state_lock);
	MultipartSnapshot TakeMultipartSnapshot() DUCKDB_EXCLUDES(state_lock);
	void RestorePartETags(vector<string> etags) DUCKDB_EXCLUDES(state_lock);
	void CompleteMultipartUpload();

private:
	reference<S3FileSystem> s3fs;
	shared_ptr<HTTPRequestSession> request_session;
	const string path;
	const string display_path;
	const S3UploadConfig config;

	annotated_mutex state_lock;
	State state DUCKDB_GUARDED_BY(state_lock) = State::OPEN;
	Operation operation DUCKDB_GUARDED_BY(state_lock) = Operation::NONE;
	idx_t next_offset DUCKDB_GUARDED_BY(state_lock) = 0;
	unique_ptr<BufferedPart> buffered_part DUCKDB_GUARDED_BY(state_lock);
	shared_ptr<const string> multipart_upload_id DUCKDB_GUARDED_BY(state_lock);
	vector<string> part_etags DUCKDB_GUARDED_BY(state_lock);
	FailureSnapshot primary_failure DUCKDB_GUARDED_BY(state_lock);
};

} // namespace duckdb
