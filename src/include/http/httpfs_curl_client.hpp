#pragma once

#include <curl/curl.h>
#include <utility>

#include "duckdb/common/http_util.hpp"

namespace duckdb {
class HTTPLogger;
class FileOpener;
struct FileOpenerInfo;
class HTTPState;

class CURLURLHandle {
public:
	CURLURLHandle();
	CURLURLHandle(const CURLURLHandle &other);
	~CURLURLHandle();

	CURLURLHandle &operator=(const CURLURLHandle &) = delete;

	CURLU *Get() {
		return handle;
	}

private:
	explicit CURLURLHandle(CURLU *handle_p);

	CURLU *handle;
};

class CURLHandle {
public:
	CURLHandle(const string &token, const string &cert_path);
	~CURLHandle();

public:
	operator CURL *() { // NOLINT(google-explicit-constructor)
		return curl;
	}
	CURLcode Execute() {
		return curl_easy_perform(curl);
	}

private:
	CURL *curl = nullptr;
};

class CURLRequestHeaders {
public:
	CURLRequestHeaders() {
	}
	CURLRequestHeaders(CURLRequestHeaders &&other) noexcept {
		headers = other.headers;
		other.headers = nullptr;
	}
	CURLRequestHeaders &operator=(CURLRequestHeaders &&other) noexcept {
		std::swap(headers, other.headers);
		return *this;
	}
	CURLRequestHeaders(const CURLRequestHeaders &) = delete;
	CURLRequestHeaders &operator=(const CURLRequestHeaders &) = delete;

	~CURLRequestHeaders() {
		if (headers) {
			curl_slist_free_all(headers);
		}
		headers = nullptr;
	}
	explicit operator bool() const {
		return headers != nullptr;
	}

public:
	void Add(const string &header) {
		headers = curl_slist_append(headers, header.c_str());
	}

public:
	curl_slist *headers = nullptr;
};

} // namespace duckdb
