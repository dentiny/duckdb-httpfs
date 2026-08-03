#include "s3/s3_upload_session.hpp"

#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"
#include "s3/s3_xml_response.hpp"
#include "s3/s3fs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstring>
#include <sstream>

namespace duckdb {

namespace {

class S3AmbiguousUploadException : public IOException {
public:
	explicit S3AmbiguousUploadException(const string &message) : IOException(message) {
	}
};

struct S3UploadErrorUtil {
	static string RedactPath(const string &path) {
		auto query_position = path.find('?');
		return query_position == string::npos ? path : path.substr(0, query_position);
	}
};

static bool IsSuccessfulStatus(HTTPStatusCode status) {
	auto status_code = static_cast<int>(status);
	return status_code >= 200 && status_code < 300;
}

} // namespace

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
    : s3fs(s3fs_p), request_session(std::move(request_session_p)), path(std::move(path_p)),
      display_path(S3UploadErrorUtil::RedactPath(path)), config(config_p) {
}

S3UploadSession::~S3UploadSession() noexcept = default;

S3UploadSession::FailureSnapshot S3UploadSession::CaptureFailure() const DUCKDB_REQUIRES(state_lock) {
	D_ASSERT(state == State::FAILED || state == State::AMBIGUOUS);
	D_ASSERT(primary_failure.primary_error);
	return primary_failure;
}

void S3UploadSession::ThrowFailure(const FailureSnapshot &failure) {
	D_ASSERT(failure.primary_error);
	if (!failure.abort_error) {
		failure.primary_error->Throw();
	}
	throw Exception(failure.primary_error->ExtraInfo(), failure.primary_error->Type(),
	                failure.primary_error->RawMessage() + "\n\nAdditionally, " + failure.abort_error->RawMessage());
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::BeginWrite(idx_t location, idx_t size)
    DUCKDB_EXCLUDES(state_lock) {
	enum class LocalFailure : uint8_t { NONE, NON_SEQUENTIAL_WRITE, FILE_SIZE_LIMIT };

	const char *error_message = nullptr;
	FailureSnapshot failure;
	LocalFailure local_failure = LocalFailure::NONE;
	idx_t expected_offset = 0;
	unique_ptr<BufferedPart> result;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (operation != Operation::NONE) {
			error_message = "Concurrent S3 upload operations are not supported";
		} else if (state == State::FAILED || state == State::AMBIGUOUS) {
			failure = CaptureFailure();
		} else if (state == State::FINALIZED) {
			error_message = "Cannot write to a finalized S3 upload";
		} else if (location != next_offset) {
			operation = Operation::WRITE;
			local_failure = LocalFailure::NON_SEQUENTIAL_WRITE;
			expected_offset = next_offset;
		} else if (size > config.max_file_size - next_offset) {
			operation = Operation::WRITE;
			local_failure = LocalFailure::FILE_SIZE_LIMIT;
		} else {
			operation = Operation::WRITE;
			result = std::move(buffered_part);
		}
	}
	if (failure.primary_error) {
		ThrowFailure(failure);
	}
	if (local_failure == LocalFailure::NON_SEQUENTIAL_WRITE) {
		FailOperation(ErrorData(ExceptionType::IO,
		                        StringUtil::Format("S3 writes must be sequential: expected offset %llu, got %llu",
		                                           expected_offset, location)),
		              FailureDisposition::DEFINITIVE);
	}
	if (local_failure == LocalFailure::FILE_SIZE_LIMIT) {
		FailOperation(ErrorData(ExceptionType::IO,
		                        StringUtil::Format("S3 upload exceeds the configured maximum file size of %llu bytes",
		                                           config.max_file_size)),
		              FailureDisposition::DEFINITIVE);
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
		if (operation != Operation::NONE) {
			error_message = "Concurrent S3 upload operations are not supported";
		} else if (state == State::FAILED || state == State::AMBIGUOUS) {
			failure = CaptureFailure();
		} else if (state == State::FINALIZED) {
			already_finalized = true;
		} else {
			operation = Operation::FINALIZE;
			result = std::move(buffered_part);
		}
	}
	if (failure.primary_error) {
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

void S3UploadSession::FailOperation(ErrorData error, FailureDisposition disposition) DUCKDB_EXCLUDES(state_lock) {
	auto stored_error = make_shared_ptr<const ErrorData>(std::move(error));
	shared_ptr<const string> upload_id;
	FailureSnapshot failure;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (state != State::FAILED && state != State::AMBIGUOUS) {
			D_ASSERT(!primary_failure.primary_error);
			primary_failure.primary_error = std::move(stored_error);
			state = disposition == FailureDisposition::DEFINITIVE ? State::FAILED : State::AMBIGUOUS;
			if (state == State::FAILED) {
				upload_id = multipart_upload_id;
			}
		}
	}

	shared_ptr<const ErrorData> abort_error;
	if (upload_id) {
		abort_error = AbortMultipartUpload(*upload_id);
	}
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (abort_error && !primary_failure.abort_error) {
			primary_failure.abort_error = std::move(abort_error);
		}
		operation = Operation::NONE;
		failure = CaptureFailure();
	}
	ThrowFailure(failure);
}

shared_ptr<const ErrorData> S3UploadSession::AbortMultipartUpload(const string &upload_id) {
	auto query_param = "uploadId=" + S3Url::Encode(upload_id, true);
	try {
		auto response = s3fs.get().DeleteRequest(*request_session, path, std::move(query_param));
		if (response->status == HTTPStatusCode::NoContent_204) {
			return nullptr;
		}
		auto message =
		    StringUtil::Format("Failed to abort S3 multipart upload for \"%s\": HTTP %d%s", display_path,
		                       static_cast<int>(response->status), S3RequestUtil::ParseError(response->body));
		return make_shared_ptr<const ErrorData>(ExceptionType::HTTP, std::move(message));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto message = StringUtil::Format(
		    "Failed to abort S3 multipart upload for \"%s\": the cleanup request could not be completed", display_path);
		return make_shared_ptr<const ErrorData>(error.Type(), std::move(message));
	}
}

unique_ptr<S3UploadSession::BufferedPart> S3UploadSession::AllocateBufferedPart() {
	auto capacity = CurrentPartSize();
	auto buffer = s3fs.get().buffer_manager.Allocate(MemoryTag::EXTENSION, capacity);
	return make_uniq<BufferedPart>(std::move(buffer), capacity);
}

idx_t S3UploadSession::AppendToBufferedPart(BufferedPart &buffered_part_p, const_data_ptr_t data, idx_t size) {
	D_ASSERT(buffered_part_p.size < buffered_part_p.capacity);
	auto copy_size = MinValue<idx_t>(size, buffered_part_p.capacity - buffered_part_p.size);
	memcpy(buffered_part_p.Ptr() + buffered_part_p.size, data, copy_size);
	buffered_part_p.size += copy_size;
	return copy_size;
}

idx_t S3UploadSession::CurrentPartSize() DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	return config.PartSize(part_etags.size());
}

bool S3UploadSession::ShouldBufferUnbufferedSpan(idx_t size) DUCKDB_EXCLUDES(state_lock) {
	annotated_lock_guard<annotated_mutex> guard(state_lock);
	D_ASSERT(state == State::OPEN || state == State::MULTIPART_ACTIVE);
	auto part_size = config.PartSize(part_etags.size());
	return state == State::OPEN ? size <= part_size : size < part_size;
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

string S3UploadSession::InitializeMultipartUpload() {
	string result;
	auto response = s3fs.get().PostRequest(*request_session, path, result, nullptr, 0,
	                                       "uploads=", S3PostRequestMode::NON_REPLAYABLE);
	if (response->HasRequestError()) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload initialization for \"%s\" has an unknown outcome because the response was not "
		    "received; the request was not retried and cannot be aborted without an upload ID",
		    display_path));
	}
	if (!IsSuccessfulStatus(response->status)) {
		throw HTTPException(StringUtil::Format("S3 multipart upload initialization for \"%s\" failed with HTTP %d%s",
		                                       display_path, static_cast<int>(response->status),
		                                       S3RequestUtil::ParseError(response->body)));
	}

	S3XMLResponse parsed_response;
	if (!S3XMLResponseParser::TryParse(result, parsed_response)) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload initialization for \"%s\" returned malformed XML; the request was not retried and "
		    "cannot be aborted without an upload ID",
		    display_path));
	}
	if (parsed_response.type != S3XMLResponseType::MULTIPART_INITIALIZATION) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload initialization for \"%s\" returned an unrecognized response; the request was not "
		    "retried and cannot be aborted without an upload ID",
		    display_path));
	}
	return parsed_response.upload_id;
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
	if (response->HasRequestError()) {
		throw IOException("S3 upload request for \"%s\" could not be completed", display_path);
	}
	if (response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Unable to connect to URL %s: %s (HTTP code %d)%s", display_path,
		                    response->GetError(), static_cast<int>(response->status),
		                    S3RequestUtil::ParseError(response->body));
	}
	if (!response->headers.HasHeader("ETag")) {
		throw IOException("Unexpected response when uploading to S3");
	}
	return response->headers.GetHeaderValue("ETag");
}

void S3UploadSession::UploadPart(const_data_ptr_t data, idx_t size) {
	idx_t part_number;
	shared_ptr<const string> upload_id;
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		if (!config.HasPartCapacity(part_etags.size())) {
			throw IOException("S3 upload exceeds the configured maximum of %llu multipart parts", config.max_parts);
		}
		part_number = part_etags.size() + 1;
	}
	EnsureMultipartUpload();
	{
		annotated_lock_guard<annotated_mutex> guard(state_lock);
		D_ASSERT(state == State::MULTIPART_ACTIVE);
		D_ASSERT(part_number == part_etags.size() + 1);
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
	auto response = s3fs.get().PostRequest(*request_session, path, result, const_data_ptr_cast(completion_body.data()),
	                                       completion_body.size(), query_param, S3PostRequestMode::NON_REPLAYABLE);
	if (response->HasRequestError()) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload completion for \"%s\" has an unknown outcome because the response was not received; "
		    "the request was not retried or aborted",
		    display_path));
	}
	if (!IsSuccessfulStatus(response->status)) {
		throw HTTPException(StringUtil::Format("S3 multipart upload completion for \"%s\" failed with HTTP %d%s",
		                                       display_path, static_cast<int>(response->status),
		                                       S3RequestUtil::ParseError(response->body)));
	}

	S3XMLResponse parsed_response;
	if (!S3XMLResponseParser::TryParse(result, parsed_response)) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload completion for \"%s\" returned malformed XML; the request was not retried or aborted",
		    display_path));
	}
	if (parsed_response.type == S3XMLResponseType::ERROR) {
		throw HTTPException(StringUtil::Format(
		    "S3 multipart upload completion for \"%s\" failed: %s%s%s", display_path,
		    parsed_response.error_code.empty() ? "S3 returned an embedded error" : parsed_response.error_code,
		    parsed_response.error_message.empty() ? "" : ": ", parsed_response.error_message));
	}
	if (parsed_response.type != S3XMLResponseType::MULTIPART_COMPLETION) {
		throw S3AmbiguousUploadException(StringUtil::Format(
		    "S3 multipart upload completion for \"%s\" returned an unrecognized response; the request was not retried "
		    "or aborted",
		    display_path));
	}
}

S3UploadSession::WriteClaim S3UploadSession::Write(const_data_ptr_t data, idx_t size, idx_t location) {
	auto local_buffered_part = BeginWrite(location, size);
	try {
		idx_t input_offset = 0;
		if (local_buffered_part && size > 0) {
			if (local_buffered_part->size == local_buffered_part->capacity) {
				UploadBufferedPart(*local_buffered_part);
				local_buffered_part.reset();
			} else {
				input_offset = AppendToBufferedPart(*local_buffered_part, data, size);
				if (local_buffered_part->size == local_buffered_part->capacity && input_offset < size) {
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
	} catch (S3AmbiguousUploadException &ex) {
		FailOperation(ErrorData(ex), FailureDisposition::AMBIGUOUS);
	} catch (std::exception &ex) {
		FailOperation(ErrorData(ex), FailureDisposition::DEFINITIVE);
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
			} else {
				UploadSingle(*local_buffered_part);
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
	} catch (S3AmbiguousUploadException &ex) {
		FailOperation(ErrorData(ex), FailureDisposition::AMBIGUOUS);
	} catch (std::exception &ex) {
		FailOperation(ErrorData(ex), FailureDisposition::DEFINITIVE);
	}
}

} // namespace duckdb
