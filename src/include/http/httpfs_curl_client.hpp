#pragma once

#include <curl/curl.h>
#include <utility>

#include "http/httpfs_client.hpp"

namespace duckdb {
class HTTPLogger;
class FileOpener;
struct FileOpenerInfo;
class HTTPState;

class CURLURLHandle {
private:
	explicit CURLURLHandle(CURLU *handle_p);

public:
	CURLURLHandle();
	CURLURLHandle(const CURLURLHandle &other);
	~CURLURLHandle();

	CURLURLHandle &operator=(const CURLURLHandle &) = delete;

public:
	CURLU *Get() {
		return handle;
	}

private:
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

public:
	explicit operator bool() const {
		return headers != nullptr;
	}

	void Add(const string &header) {
		headers = curl_slist_append(headers, header.c_str());
	}
	void Add(const string &name, const string &value) {
		if (HTTPFSHeaderValue::IsEmpty(value)) {
			Add(name + ";");
		} else {
			Add(name + ": " + value);
		}
	}

public:
	curl_slist *headers = nullptr;
};

} // namespace duckdb
