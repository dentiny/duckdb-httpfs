#include "http/httpfs.hpp"

#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/time.hpp"

namespace duckdb {

static string StripETagQuotes(const string &etag) {
	if (etag.size() >= 2 && etag.front() == '"' && etag.back() == '"') {
		return etag.substr(1, etag.size() - 2);
	}
	return etag;
}

static void ApplyReadCondition(HTTPHeaders &headers, const HTTPReadConfig &read_config) {
	if (read_config.condition.type == HTTPReadConditionType::ETAG) {
		headers["If-Match"] = read_config.condition.value;
	}
}

static HTTPHeaders RemoveRangeHeader(const HTTPHeaders &headers) {
	HTTPHeaders result;
	for (const auto &header : headers) {
		if (!StringUtil::CIEquals(header.first, "Range")) {
			result[header.first] = header.second;
		}
	}
	return result;
}

static HTTPHeaders PrepareFullGetHeaders(const HTTPHeaders &headers, const HTTPReadConfig &read_config) {
	auto result = RemoveRangeHeader(headers);
	ApplyReadCondition(result, read_config);
	return result;
}

static unique_ptr<HTTPResponse> SendSessionRequest(HTTPRequestSession &session,
                                                   const CapturedHTTPRequestSnapshot &captured,
                                                   HTTPFSParams &request_params, BaseRequest &request) {
	auto lease = session.AcquireClient(captured, request_params, request.proto_host_port);
	try {
		auto response = request_params.http_util.Request(request, lease.Client());
		// A completed HTTP response leaves the transport reusable, regardless of its status.
		if (response && response->HasRequestError()) {
			lease.Invalidate();
		}
		return response;
	} catch (...) {
		lease.Invalidate();
		throw;
	}
}

unique_ptr<HTTPResponse> HTTPFileSystem::RunHeadRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
                                                        HTTPSendCallback send_request) {
	auto request_headers = RemoveRangeHeader(header_map);
	http_params.extra_headers.erase("Range");
	HeadRequestInfo head_request(url, request_headers, http_params);
	return send_request(head_request);
}

unique_ptr<HTTPResponse> HTTPFileSystem::RunDeleteRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
                                                          HTTPSendCallback send_request) {
	DeleteRequestInfo delete_request(url, header_map, http_params);
	return send_request(delete_request);
}

unique_ptr<HTTPResponse> HTTPFileSystem::RunPostRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
                                                        string &buffer_out, char *buffer_in, idx_t buffer_in_len,
                                                        HTTPSendCallback send_request) {
	PostRequestInfo post_request(url, header_map, http_params, const_data_ptr_cast(buffer_in), buffer_in_len);
	auto result = send_request(post_request);
	buffer_out = std::move(post_request.buffer_out);
	return result;
}

unique_ptr<HTTPResponse> HTTPFileSystem::RunPutRequest(string url, HTTPHeaders header_map, HTTPFSParams &http_params,
                                                       char *buffer_in, idx_t buffer_in_len, const string &content_type,
                                                       HTTPSendCallback send_request) {
	PutRequestInfo put_request(url, header_map, http_params, const_data_ptr_cast(buffer_in), buffer_in_len,
	                           content_type);
	return send_request(put_request);
}

unique_ptr<HTTPResponse> HTTPFileSystem::RunGetRequest(HTTPFileHandle &hfh, string url, HTTPHeaders header_map,
                                                       HTTPFSParams &http_params, const HTTPReadConfig &read_config,
                                                       CachedFileDownload &download, HTTPErrorCallback get_error,
                                                       HTTPSendCallback send_request) {
	auto request_headers = PrepareFullGetHeaders(header_map, read_config);
	http_params.extra_headers.erase("Range");
	GetRequestInfo get_request(
	    url, request_headers, http_params,
	    [&](const HTTPResponse &response) {
		    if (response.status == HTTPStatusCode::PreconditionFailed_412 &&
		        read_config.condition.type == HTTPReadConditionType::ETAG) {
			    return false;
		    }
		    if (static_cast<int>(response.status) >= 400) {
			    throw get_error(response);
		    }
		    if (static_cast<int>(response.status) < 300) {
			    ValidateResponseETag(hfh, read_config, response);
		    }
		    download.Reset();
		    optional_idx content_length;
		    if (response.HasHeader("Content-Length")) {
			    try {
				    content_length = std::stoull(response.GetHeaderValue("Content-Length"));
			    } catch (const std::exception &) {
				    // Grow the buffer incrementally when Content-Length is not numeric.
			    }
		    }
		    if (content_length.IsValid() && content_length.GetIndex() > 0) {
			    download.Reserve(content_length.GetIndex());
		    }
		    return true;
	    },
	    [&](const_data_ptr_t data, idx_t data_length) {
		    download.Append(data, data_length);
		    return true;
	    });

	return send_request(get_request);
}

static void SetRangeRequestNotSupported(HTTPResponse &response) {
	try {
		RangeRequestNotSupportedException::Throw();
	} catch (HTTPException &ex) {
		response.request_error = ex.what();
		response.success = false;
	}
}

unique_ptr<HTTPResponse>
HTTPFileSystem::RunGetRangeRequest(HTTPFileHandle &hfh, string url, HTTPHeaders header_map, HTTPFSParams &http_params,
                                   const HTTPReadConfig &read_config, idx_t file_offset, char *buffer_out,
                                   idx_t buffer_out_len, HTTPErrorCallback get_error, HTTPSendCallback send_request) {
	string range_expr = "bytes=" + to_string(file_offset) + "-" + to_string(file_offset + buffer_out_len - 1);
	header_map["Range"] = range_expr;
	ApplyReadCondition(header_map, read_config);

	D_ASSERT(hfh.file_state);
	auto range_request = hfh.file_state->BeginRangeRequest(read_config.auto_fallback_to_full_download);
	if (range_request.Support() == RangeRequestSupport::NOT_SUPPORTED && read_config.auto_fallback_to_full_download) {
		auto response = make_uniq<HTTPResponse>(HTTPStatusCode::INVALID);
		SetRangeRequestNotSupported(*response);
		return response;
	}

	idx_t out_offset = 0;
	bool range_request_not_supported = false;
	GetRequestInfo get_request(
	    url, header_map, http_params,
	    [&](const HTTPResponse &response) {
		    if (response.status == HTTPStatusCode::PreconditionFailed_412 &&
		        read_config.condition.type == HTTPReadConditionType::ETAG) {
			    return false;
		    }
		    if (static_cast<int>(response.status) >= 400) {
			    throw get_error(response);
		    }
		    if (static_cast<int>(response.status) < 300) {
			    out_offset = 0;
			    ValidateResponseETag(hfh, read_config, response);

			    if (response.HasHeader("Content-Length")) {
				    unsigned long long content_length;
				    bool parsed = false;
				    try {
					    content_length = stoull(response.GetHeaderValue("Content-Length"));
					    parsed = true;
				    } catch (const std::exception &) {
					    // Content-Length header contains a non-numeric value, so skip validation.
				    }
				    if (parsed && (idx_t)content_length != buffer_out_len) {
					    range_request_not_supported = true;
					    range_request.MarkNotSupported();
					    return false;
				    }
			    }
			    if (response.status == HTTPStatusCode::PartialContent_206) {
				    range_request.MarkSupported();
			    }
		    }
		    return true;
	    },
	    [&](const_data_ptr_t data, idx_t data_length) {
		    if (buffer_out != nullptr) {
			    if (data_length + out_offset > buffer_out_len) {
				    throw HTTPException("Server sent back more data than expected, `SET force_download=true` might "
				                        "help in this case");
			    }
			    memcpy(buffer_out + out_offset, data, data_length);
			    out_offset += data_length;
		    }
		    return true;
	    });

	get_request.try_request = read_config.auto_fallback_to_full_download;
	auto response = send_request(get_request);
	if (range_request_not_supported) {
		SetRangeRequestNotSupported(*response);
		return response;
	}
	if (response && !response->HasRequestError() && response->Success() && buffer_out != nullptr &&
	    out_offset != buffer_out_len) {
		throw IOException("Short read for HTTP GET to '%s': requested range %s (%llu bytes), but received %llu bytes",
		                  url, range_expr, static_cast<unsigned long long>(buffer_out_len),
		                  static_cast<unsigned long long>(out_offset));
	}

	if (response && (response->Success() || response->status == HTTPStatusCode::PartialContent_206 ||
	                 response->status == HTTPStatusCode::Accepted_202)) {
		range_request.MarkSupported();
		const auto elapsed_nanos =
		    TimePoint::ElapsedNanos(get_request.request_monotonic_start, get_request.request_monotonic_end);
		const double total_seconds = elapsed_nanos > 0 ? static_cast<double>(elapsed_nanos) / 1e9 : 0;
		const idx_t bytes = get_request.bytes_received != 0 ? get_request.bytes_received : buffer_out_len;
		hfh.RecordNetworkSample(total_seconds, bytes, get_request.have_time_to_fst_byte,
		                        get_request.time_to_fst_byte_sec);
	}
	return response;
}

unique_ptr<HTTPResponse> HTTPFileSystem::HeadRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	auto captured = hfh.request_session->Capture();
	captured.snapshot->AddConfiguredHeaders(header_map);
	auto request_params = captured.snapshot->CreateRequestParams();
	return RunHeadRequest(url, header_map, *request_params, [&](BaseRequest &request) {
		return SendSessionRequest(*hfh.request_session, captured, *request_params, request);
	});
}

unique_ptr<HTTPResponse> HTTPFileSystem::DeleteRequest(FileHandle &handle, string url, HTTPHeaders header_map) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	auto captured = hfh.request_session->Capture();
	captured.snapshot->AddConfiguredHeaders(header_map);
	auto request_params = captured.snapshot->CreateRequestParams();
	return RunDeleteRequest(url, header_map, *request_params, [&](BaseRequest &request) {
		return SendSessionRequest(*hfh.request_session, captured, *request_params, request);
	});
}

HTTPException HTTPFileSystem::GetHTTPError(FileHandle &, const HTTPResponse &response, const string &url) {
	auto status_message = HTTPFSUtil::GetStatusMessage(response.status);
	string error = "HTTP GET error on '" + url + "' (HTTP " + to_string(static_cast<int>(response.status)) + " " +
	               status_message + ")";
	if (response.status == HTTPStatusCode::RangeNotSatisfiable_416) {
		error += " This could mean the file was changed. Try disabling the duckdb http metadata cache "
		         "if enabled, and confirm the server supports range requests.";
	}
	return HTTPException(response, error);
}

void HTTPFileSystem::ValidateResponseETag(HTTPFileHandle &hfh, const HTTPReadConfig &read_config,
                                          const HTTPResponse &response) {
	if (!read_config.validate_etag || read_config.etag.empty() || !response.HasHeader("ETag")) {
		return;
	}
	auto response_etag = response.GetHeaderValue("ETag");
	if (response_etag.empty() || StripETagQuotes(response_etag) == StripETagQuotes(read_config.etag)) {
		return;
	}
	EraseGlobalCacheEntry(hfh.path);
	throw HTTPException(
	    response,
	    "ETag on reading file \"%s\" was initially %s and now it returned %s, this likely means "
	    "the remote file has changed.\nFor parquet or similar single table sources, consider "
	    "retrying the query, for persistent FileHandles such as databases consider `DETACH` and re-`ATTACH` "
	    "\nYou can disable checking etags via `SET unsafe_disable_etag_checks = true;`",
	    hfh.path, read_config.etag, response_etag);
}

void HTTPFileSystem::ThrowIfReadConditionFailed(HTTPFileHandle &hfh, const HTTPReadConfig &read_config,
                                                const HTTPResponse &response) {
	if (response.status != HTTPStatusCode::PreconditionFailed_412 ||
	    read_config.condition.type != HTTPReadConditionType::ETAG) {
		return;
	}
	EraseGlobalCacheEntry(hfh.path);
	throw HTTPException(response, "ETag on reading file \"%s\" changed after it was opened: the server rejected %s",
	                    hfh.path, read_config.condition.value);
}

unique_ptr<HTTPResponse> HTTPFileSystem::GetRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                    const HTTPReadConfig &read_config, CachedFileDownload &download) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	auto captured = hfh.request_session->Capture();
	captured.snapshot->AddConfiguredHeaders(header_map);
	auto request_params = captured.snapshot->CreateRequestParams();
	return RunGetRequest(
	    hfh, url, header_map, *request_params, read_config, download,
	    [&](const HTTPResponse &response) { return GetHTTPError(handle, response, url); },
	    [&](BaseRequest &request) {
		    return SendSessionRequest(*hfh.request_session, captured, *request_params, request);
	    });
}

unique_ptr<HTTPResponse> HTTPFileSystem::GetRangeRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                                         const HTTPReadConfig &read_config, idx_t file_offset,
                                                         char *buffer_out, idx_t buffer_out_len) {
	auto &hfh = handle.Cast<HTTPFileHandle>();
	auto captured = hfh.request_session->Capture();
	captured.snapshot->AddConfiguredHeaders(header_map);
	auto request_params = captured.snapshot->CreateRequestParams();
	return RunGetRangeRequest(
	    hfh, url, header_map, *request_params, read_config, file_offset, buffer_out, buffer_out_len,
	    [&](const HTTPResponse &response) { return GetHTTPError(handle, response, url); },
	    [&](BaseRequest &request) {
		    return SendSessionRequest(*hfh.request_session, captured, *request_params, request);
	    });
}

} // namespace duckdb
