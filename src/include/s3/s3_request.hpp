#pragma once

#include "http/httpfs.hpp"
#include "s3/s3_url.hpp"

#include "duckdb/common/encryption_state.hpp"

#include <functional>
#include <unordered_map>

namespace duckdb {

class ClientContext;
class S3FileHandle;

struct S3RefreshableHTTPParams {
	string http_proxy;
	idx_t http_proxy_port = 0;
	string http_proxy_username;
	string http_proxy_password;
	unordered_map<string, string> extra_headers;
	bool override_verify_ssl = false;
	bool verify_ssl = true;
	string bearer_token;
};

struct S3RequestSnapshot : public HTTPRequestSnapshot {
	static constexpr HTTPRequestSnapshotType TYPE = HTTPRequestSnapshotType::S3;

	S3RequestSnapshot(const HTTPFSParams &http_params, const S3AuthParams &auth_params_p, string refresh_path_p,
	                  weak_ptr<ClientContext> client_context_p = {}, bool credential_refresh_enabled_p = true,
	                  bool region_redirected_p = false, idx_t credential_generation_p = 0);

	S3AuthParams auth_params;
	string refresh_path;
	weak_ptr<ClientContext> client_context;
	bool credential_refresh_enabled;
	bool region_redirected;
	idx_t credential_generation;
};

struct S3RequestData {
	S3AuthParams auth_params;
	unique_ptr<HTTPParams> http_params;
	CapturedHTTPRequestSnapshot captured;
	string source_url;
	string http_url;
	HTTPHeaders headers;
};

struct S3RequestUtil {
	static HTTPHeaders CreateHeader(EncryptionUtil &encryption_util, string url, string query, string host,
	                                string service, string method, const S3AuthParams &auth_params,
	                                string date_now = "", string datetime_now = "", string payload_hash = "",
	                                string content_type = "", string content_md5 = "");
	static string GetPayloadHash(EncryptionUtil &encryption_util, const_data_ptr_t buffer, idx_t buffer_len);
	static bool IsRequestTimeout(const HTTPResponse &response);

	static optional_idx TryFindTagContents(const string &response, const string &tag, idx_t cur_pos, string &result);
	static optional_idx FindTagContents(const string &response, const string &tag, idx_t cur_pos, string &result);
	static string GetBadRequestError(const S3AuthParams &auth_params, const string &correct_region = "");
	static string GetAuthError(const S3AuthParams &auth_params);
	static string GetGCSAuthError(const S3AuthParams &auth_params);
	static string ParseError(const string &error);
	static HTTPException GetError(const S3AuthParams &auth_params, const HTTPResponse &response, const string &url);
	static HTTPException GetRequestError(const S3RequestData &request_data, const HTTPResponse &response);
};

struct S3RequestExecutor {
	using CreateDataCallback = std::function<S3RequestData()>;
	using RequestCallback = std::function<unique_ptr<HTTPResponse>(S3RequestData &)>;
	using RefreshCallback = std::function<bool(const S3RequestData &)>;
	using SetRegionCallback = std::function<void(const S3RequestData &, const string &)>;

	static unique_ptr<HTTPResponse> Run(const string &s3_url, const CreateDataCallback &create_data,
	                                    bool transient_retry_eligible, const RequestCallback &request,
	                                    const RefreshCallback &refresh_auth_params,
	                                    const SetRegionCallback &set_region);
	static unique_ptr<HTTPResponse> RunSession(EncryptionUtil &encryption_util, HTTPRequestSession &session,
	                                           const string &s3_url, const string &method, const string &query_string,
	                                           const string &payload_hash, const string &content_type,
	                                           const string &content_md5, const RequestCallback &request);
	static unique_ptr<HTTPResponse> RunHandle(EncryptionUtil &encryption_util, S3FileHandle &handle,
	                                          const string &s3_url, const string &method, const string &version_id,
	                                          const RequestCallback &request);

	static bool TryRefreshSession(HTTPRequestSession &session, const S3RequestData &request_data);
	static bool SetSessionRegion(HTTPRequestSession &session, const string &correct_region, string &previous_region);
	static bool CredentialRefreshEnabled(optional_ptr<FileOpener> opener);
	static S3RefreshableHTTPParams ReadRefreshableHTTPParams(optional_ptr<FileOpener> opener, const string &path);
	static shared_ptr<HTTPRequestSession> CreateSession(optional_ptr<FileOpener> opener, const string &path,
	                                                    const S3AuthParams &auth_params);

	static unique_ptr<HTTPResponse> SendSessionRequest(HTTPRequestSession &session,
	                                                   const CapturedHTTPRequestSnapshot &captured,
	                                                   HTTPFSParams &params, BaseRequest &request);
	static unique_ptr<HTTPResponse> SendHandleRequest(S3FileHandle &handle, const CapturedHTTPRequestSnapshot &captured,
	                                                  HTTPFSParams &params, BaseRequest &request);

private:
	static S3RequestData CreateRequestData(EncryptionUtil &encryption_util, const CapturedHTTPRequestSnapshot &captured,
	                                       const string &s3_url, const string &method, const string &query_string,
	                                       const string &payload_hash = "", const string &content_type = "",
	                                       const string &content_md5 = "");
	static S3RequestData CreateHandleRequestData(EncryptionUtil &encryption_util, S3FileHandle &handle,
	                                             const string &s3_url, const string &method, const string &version_id);
};

} // namespace duckdb
