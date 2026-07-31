#include "s3/s3_upload_session.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3fs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

#include <cstring>
#include <sstream>

namespace duckdb {

S3UploadSession::WriteClaim::WriteClaim(S3UploadSession &session_p) : session(session_p) {
}

S3UploadSession::WriteClaim::WriteClaim(WriteClaim &&other) noexcept
    : session(other.session), finished(other.finished) {
	other.finished = true;
}

S3UploadSession::WriteClaim::~WriteClaim() {
	Finish();
}

void S3UploadSession::WriteClaim::Finish() {
	if (finished) {
		return;
	}
	session.get().ReleaseWrite();
	finished = true;
}

S3UploadSession::S3UploadSession(S3FileSystem &s3fs_p, shared_ptr<HTTPRequestSession> request_session_p, string path_p,
                                 S3UploadConfig config_p)
    : s3fs(s3fs_p), request_session(std::move(request_session_p)), path(std::move(path_p)), config(config_p) {
}

S3UploadSession::FailureSnapshot S3UploadSession::CaptureFailure() const DUCKDB_REQUIRES(state_lock) {
	D_ASSERT(state == State::FAILED);
	return primary_failure;
}

void S3UploadSession::ThrowFailure(const FailureSnapshot &failure) {
	switch (failure.type) {
	case FailureType::ERROR_DATA:
		D_ASSERT(failure.error);
		failure.error->Throw();
	case FailureType::NON_SEQUENTIAL_WRITE:
		throw IOException("S3 writes must be sequential: expected offset %llu, got %llu", failure.expected_offset,
		                  failure.actual_offset);
	case FailureType::SIZE_OVERFLOW:
		throw IOException("S3 upload size exceeds the supported range");
	case FailureType::NONE:
		break;
	}
	throw InternalException("S3 upload entered a failed state without an error");
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::BeginWrite(idx_t location, idx_t size)
    DUCKDB_EXCLUDES(state_lock) {
	const char *error_message = nullptr;
	FailureSnapshot failure;
	unique_ptr<BufferedPart> result;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (state == State::FAILED) {
			failure = CaptureFailure();
		} else if (state == State::FINALIZED) {
			error_message = "Cannot write to a finalized S3 upload";
		} else if (operation != Operation::NONE) {
			error_message = "Concurrent S3 upload operations are not supported";
		} else if (location != next_offset) {
			primary_failure.type = FailureType::NON_SEQUENTIAL_WRITE;
			primary_failure.expected_offset = next_offset;
			primary_failure.actual_offset = location;
			state = State::FAILED;
			failure = CaptureFailure();
		} else if (size > NumericLimits<idx_t>::Maximum() - next_offset) {
			primary_failure.type = FailureType::SIZE_OVERFLOW;
			state = State::FAILED;
			failure = CaptureFailure();
		} else {
			operation = Operation::WRITE;
			result = std::move(buffered_part);
		}
	}
	if (failure.type != FailureType::NONE) {
		ThrowFailure(failure);
	}
	if (error_message) {
		throw IOException(error_message);
	}
	return result;
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::BeginFinalize(bool &already_finalized)
    DUCKDB_EXCLUDES(state_lock) {
	const char *error_message = nullptr;
	FailureSnapshot failure;
	unique_ptr<BufferedPart> result;
	already_finalized = false;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (state == State::FAILED) {
			failure = CaptureFailure();
		} else if (state == State::FINALIZED) {
			already_finalized = true;
		} else if (operation != Operation::NONE) {
			error_message = "Concurrent S3 upload operations are not supported";
		} else {
			operation = Operation::FINALIZE;
			result = std::move(buffered_part);
		}
	}
	if (failure.type != FailureType::NONE) {
		ThrowFailure(failure);
	}
	if (error_message) {
		throw IOException(error_message);
	}
	return result;
}

void S3UploadSession::StoreWriteResult(unique_ptr<BufferedPart> buffered_part_p, idx_t size)
    DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(operation == Operation::WRITE);
	D_ASSERT(state == State::OPEN || state == State::MULTIPART_ACTIVE);
	D_ASSERT(!buffered_part);
	buffered_part = std::move(buffered_part_p);
	next_offset += size;
}

void S3UploadSession::ReleaseWrite() DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(operation == Operation::WRITE);
	operation = Operation::NONE;
}

void S3UploadSession::FinishFinalize() DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(operation == Operation::FINALIZE);
	state = State::FINALIZED;
	operation = Operation::NONE;
}

void S3UploadSession::FailOperation(ErrorData error) DUCKDB_EXCLUDES(state_lock) {
	auto stored_error = make_shared_ptr<const ErrorData>(std::move(error));
	FailureSnapshot failure;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (state != State::FAILED) {
			D_ASSERT(primary_failure.type == FailureType::NONE);
			primary_failure.type = FailureType::ERROR_DATA;
			primary_failure.error = std::move(stored_error);
			state = State::FAILED;
		}
		operation = Operation::NONE;
		failure = CaptureFailure();
	}
	ThrowFailure(failure);
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::AllocateBufferedPart() {
	auto buffer = s3fs.get().buffer_manager.Allocate(MemoryTag::EXTENSION, config.aggregation_threshold);
	return make_uniq<BufferedPart>(std::move(buffer));
}

idx_t S3UploadSession::AppendToBufferedPart(BufferedPart &buffered_part_p, const_data_ptr_t data, idx_t size) {
	D_ASSERT(buffered_part_p.size < config.aggregation_threshold);
	auto copy_size = MinValue<idx_t>(size, config.aggregation_threshold - buffered_part_p.size);
	memcpy(buffered_part_p.Ptr() + buffered_part_p.size, data, copy_size);
	buffered_part_p.size += copy_size;
	return copy_size;
}

bool S3UploadSession::ShouldBufferUnbufferedSpan(idx_t size) DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(state == State::OPEN || state == State::MULTIPART_ACTIVE);
	return state == State::OPEN ? size <= config.aggregation_threshold : size < config.aggregation_threshold;
}

void S3UploadSession::WriteUnbufferedSpan(unique_ptr<BufferedPart> &buffered_part_p, const_data_ptr_t data,
                                          idx_t size) {
	idx_t input_offset = 0;
	while (input_offset < size) {
		auto remaining = size - input_offset;
		if (ShouldBufferUnbufferedSpan(remaining)) {
			buffered_part_p = AllocateBufferedPart();
			auto copied = AppendToBufferedPart(*buffered_part_p, data + input_offset, remaining);
			D_ASSERT(copied == remaining);
			return;
		}
		auto direct_size = MinValue<idx_t>(remaining, S3UploadConfig::MAX_MULTIPART_PART_SIZE);
		UploadPart(data + input_offset, direct_size);
		input_offset += direct_size;
	}
}

bool S3UploadSession::UsesKMSKey() const {
	auto captured = request_session->Capture();
	auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
	return !snapshot.auth_params.kms_key_id.empty();
}

string S3UploadSession::InitializeMultipartUpload() {
	string result;
	auto response = s3fs.get().PostRequest(*request_session, path, result, nullptr, 0, "uploads=");
	if (response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Unable to connect to URL %s: %s (HTTP code %d)", path, response->GetError(),
		                    static_cast<int>(response->status));
	}

	auto open_tag = result.find("<UploadId>");
	auto close_tag = result.find("</UploadId>", open_tag);
	if (open_tag == string::npos || close_tag == string::npos) {
		throw HTTPException("Unexpected response while initializing S3 multipart upload");
	}
	open_tag += strlen("<UploadId>");
	return result.substr(open_tag, close_tag - open_tag);
}

void S3UploadSession::EnsureMultipartUpload() {
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (state == State::MULTIPART_ACTIVE) {
			return;
		}
		D_ASSERT(state == State::OPEN);
		D_ASSERT(operation != Operation::NONE);
	}

	auto upload_id = make_shared_ptr<const string>(InitializeMultipartUpload());
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(state == State::OPEN);
		D_ASSERT(!multipart_upload_id);
		multipart_upload_id = std::move(upload_id);
		state = State::MULTIPART_ACTIVE;
	}
}

string S3UploadSession::Upload(const_data_ptr_t data, idx_t size, const string &query_param) {
	auto response = s3fs.get().PutRequest(*request_session, path, data, size, query_param);
	if (response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Unable to connect to URL %s: %s (HTTP code %d)%s", path, response->GetError(),
		                    static_cast<int>(response->status), S3RequestUtil::ParseError(response->body));
	}
	if (!response->headers.HasHeader("ETag")) {
		throw IOException("Unexpected response when uploading to S3");
	}
	return response->headers.GetHeaderValue("ETag");
}

void S3UploadSession::UploadPart(const_data_ptr_t data, idx_t size) {
	EnsureMultipartUpload();
	idx_t part_number;
	shared_ptr<const string> upload_id;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(state == State::MULTIPART_ACTIVE);
		part_number = part_etags.size() + 1;
		upload_id = multipart_upload_id;
	}
	D_ASSERT(upload_id);
	auto query_param = "partNumber=" + to_string(part_number) + "&uploadId=" + S3Url::Encode(*upload_id, true);
	auto etag = Upload(data, size, query_param);
	StorePartETag(part_number, std::move(etag));
}

void S3UploadSession::UploadBufferedPart(BufferedPart &buffered_part_p) {
	UploadPart(buffered_part_p.Ptr(), buffered_part_p.size);
}

void S3UploadSession::StorePartETag(idx_t part_number, string etag) DUCKDB_EXCLUDES(state_lock) {
	vector<string> etags;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(part_number == part_etags.size() + 1);
		etags = std::move(part_etags);
	}
	try {
		etags.push_back(std::move(etag));
	} catch (...) {
		RestorePartETags(std::move(etags));
		throw;
	}
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(part_etags.empty());
		part_etags = std::move(etags);
	}
}

void S3UploadSession::UploadSingle(BufferedPart &buffered_part_p) {
	Upload(buffered_part_p.Ptr(), buffered_part_p.size, string());
}

void S3UploadSession::UploadEmpty() {
	const_data_ptr_t empty = nullptr;
	Upload(empty, 0, string());
}

S3UploadSession::MultipartSnapshot S3UploadSession::TakeMultipartSnapshot() DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(state == State::MULTIPART_ACTIVE);
	return {multipart_upload_id, std::move(part_etags)};
}

void S3UploadSession::RestorePartETags(vector<string> etags) DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(part_etags.empty());
	part_etags = std::move(etags);
}

void S3UploadSession::CompleteMultipartUpload() {
	auto snapshot = TakeMultipartSnapshot();
	D_ASSERT(snapshot.upload_id);
	try {
		std::stringstream body;
		body << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
		for (idx_t part_index = 0; part_index < snapshot.etags.size(); part_index++) {
			body << "<Part><ETag>" << snapshot.etags[part_index] << "</ETag><PartNumber>" << part_index + 1
			     << "</PartNumber></Part>";
		}
		body << "</CompleteMultipartUpload>";
		auto completion_body = body.str();

		string result;
		auto query_param = "uploadId=" + S3Url::Encode(*snapshot.upload_id, true);
		auto response =
		    s3fs.get().PostRequest(*request_session, path, result, const_data_ptr_cast(completion_body.data()),
		                           completion_body.size(), query_param);
		if (result.find("<CompleteMultipartUploadResult") == string::npos) {
			throw HTTPException(*response, "Unexpected response during S3 multipart upload finalization: %d\n\n%s",
			                    static_cast<int>(response->status), result);
		}
	} catch (...) {
		RestorePartETags(std::move(snapshot.etags));
		throw;
	}
}

S3UploadSession::WriteClaim S3UploadSession::Write(const_data_ptr_t data, idx_t size, idx_t location) {
	auto local_buffered_part = BeginWrite(location, size);
	try {
		idx_t input_offset = 0;
		if (local_buffered_part && size > 0) {
			if (local_buffered_part->size == config.aggregation_threshold) {
				UploadBufferedPart(*local_buffered_part);
				local_buffered_part.reset();
			} else {
				input_offset = AppendToBufferedPart(*local_buffered_part, data, size);
				if (local_buffered_part->size == config.aggregation_threshold && input_offset < size) {
					UploadBufferedPart(*local_buffered_part);
					local_buffered_part.reset();
				}
			}
		}
		if (input_offset < size) {
			WriteUnbufferedSpan(local_buffered_part, data + input_offset, size - input_offset);
		}
		StoreWriteResult(std::move(local_buffered_part), size);
		return WriteClaim(*this);
	} catch (std::exception &ex) {
		FailOperation(ErrorData(ex));
	}
}

void S3UploadSession::Finalize() {
	bool already_finalized;
	auto local_buffered_part = BeginFinalize(already_finalized);
	if (already_finalized) {
		return;
	}

	try {
		State current_state;
		{
			annotated_lock_guard<annotated_mutex> guard(state_lock);
			current_state = state;
		}
		if (current_state == State::OPEN) {
			if (!local_buffered_part) {
				UploadEmpty();
			} else if (!UsesKMSKey()) {
				UploadSingle(*local_buffered_part);
			} else {
				UploadBufferedPart(*local_buffered_part);
				local_buffered_part.reset();
				CompleteMultipartUpload();
			}
		} else {
			D_ASSERT(current_state == State::MULTIPART_ACTIVE);
			if (local_buffered_part) {
				UploadBufferedPart(*local_buffered_part);
				local_buffered_part.reset();
			}
			CompleteMultipartUpload();
		}
		FinishFinalize();
	} catch (std::exception &ex) {
		FailOperation(ErrorData(ex));
	}
}

} // namespace duckdb
