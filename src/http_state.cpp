#include "http_state.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

unique_ptr<CachedFileHandle> CachedFile::TryGetHandle() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!cached_data) {
		return nullptr;
	}
	return make_uniq<CachedFileHandle>(cached_data);
}

unique_ptr<CachedFileDownload> CachedFile::StartDownload(Allocator &allocator) {
	annotated_unique_lock<annotated_mutex> guard(lock);
	while (downloading) {
		download_complete.wait(guard, [&]() DUCKDB_REQUIRES(lock) { return !downloading; });
	}
	if (cached_data) {
		return nullptr;
	}
	auto result = unique_ptr<CachedFileDownload>(new CachedFileDownload(shared_from_this(), allocator));
	downloading = true;
	return result;
}

void CachedFile::Invalidate() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	cached_data.reset();
}

CachedFileHandle::CachedFileHandle(shared_ptr<CachedFileData> file_p) : file(std::move(file_p)) {
}

const char *CachedFileHandle::GetData() const {
	return const_char_ptr_cast(file->data.get());
}

idx_t CachedFileHandle::GetSize() const {
	return file->size;
}

CachedFileDownload::CachedFileDownload(shared_ptr<CachedFile> file_p, Allocator &allocator_p)
    : file(std::move(file_p)), allocator(allocator_p) {
}

CachedFileDownload::~CachedFileDownload() {
	Abort();
}

void CachedFileDownload::ReserveInternal(idx_t capacity) {
	if (capacity <= this->capacity) {
		return;
	}
	auto new_data = allocator.Allocate(capacity);
	if (size > 0) {
		memcpy(new_data.get(), data.get(), size);
	}
	data = std::move(new_data);
	this->capacity = capacity;
}

void CachedFileDownload::Reserve(idx_t capacity) {
	ReserveInternal(capacity);
}

void CachedFileDownload::Append(const_data_ptr_t data, idx_t length) {
	if (length == 0) {
		return;
	}
	if (length > NumericLimits<idx_t>::Maximum() - size) {
		throw OutOfMemoryException("Cached file size exceeds the maximum allocation size");
	}
	const auto required_capacity = size + length;
	if (required_capacity > capacity) {
		auto new_capacity = MaxValue<idx_t>(capacity, length);
		while (new_capacity < required_capacity) {
			if (new_capacity > NumericLimits<idx_t>::Maximum() / 2) {
				new_capacity = required_capacity;
				break;
			}
			new_capacity *= 2;
		}
		ReserveInternal(new_capacity);
	}
	memcpy(this->data.get() + size, data, length);
	size += length;
}

void CachedFileDownload::Reset() {
	data.Reset();
	capacity = 0;
	size = 0;
}

unique_ptr<CachedFileHandle> CachedFileDownload::Finalize() {
	annotated_lock_guard<annotated_mutex> guard(file->lock);
	D_ASSERT(file->downloading);
	D_ASSERT(!file->cached_data);
	auto cached_data = make_shared_ptr<CachedFileData>(std::move(data), size);
	file->cached_data = cached_data;
	file->downloading = false;
	active = false;
	file->download_complete.notify_all();
	return make_uniq<CachedFileHandle>(std::move(cached_data));
}

void CachedFileDownload::Abort() {
	if (!active) {
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(file->lock);
	D_ASSERT(file->downloading);
	file->downloading = false;
	active = false;
	file->download_complete.notify_all();
}

void HTTPState::Reset() {
	// Reset Counters
	head_count = 0;
	get_count = 0;
	put_count = 0;
	post_count = 0;
	delete_count = 0;
	total_bytes_received = 0;
	total_bytes_sent = 0;

	// Reset per-path state
	annotated_lock_guard<annotated_mutex> lock(file_states_mutex);
	file_states.clear();
}

shared_ptr<HTTPState> HTTPState::TryGetState(ClientContext &context) {
	return context.registered_state->GetOrCreate<HTTPState>("http_state");
}

shared_ptr<HTTPState> HTTPState::TryGetState(optional_ptr<FileOpener> opener) {
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (client_context) {
		return TryGetState(*client_context);
	}
	return nullptr;
}

void HTTPState::WriteProfilingInformation(std::ostream &ss) {
	string read = "in: " + StringUtil::BytesToHumanReadableString(total_bytes_received);
	string written = "out: " + StringUtil::BytesToHumanReadableString(total_bytes_sent);
	string head = "#HEAD: " + to_string(head_count);
	string get = "#GET: " + to_string(get_count);
	string put = "#PUT: " + to_string(put_count);
	string post = "#POST: " + to_string(post_count);
	string del = "#DELETE: " + to_string(delete_count);

	constexpr idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││" + QueryProfiler::DrawPadded("HTTPFS HTTP Stats", TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││                                   ││\n";
	ss << "││" + QueryProfiler::DrawPadded(read, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(written, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(head, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(get, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(put, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(post, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "││" + QueryProfiler::DrawPadded(del, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
}

//! Get per-path state, create if it does not exist
shared_ptr<HTTPFileState> HTTPState::GetFileState(const string &path) {
	annotated_lock_guard<annotated_mutex> lock(file_states_mutex);
	auto &file_state = file_states[path];
	if (!file_state) {
		file_state = make_shared_ptr<HTTPFileState>();
	}
	return file_state;
}

void HTTPState::EraseFileState(const string &path) {
	annotated_lock_guard<annotated_mutex> lock(file_states_mutex);
	file_states.erase(path);
}

} // namespace duckdb
