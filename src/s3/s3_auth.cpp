#include "s3/s3_auth.hpp"

#include "duckdb/common/string_util.hpp"

#include <cstdlib>

namespace duckdb {

void AWSEnvironmentCredentialsProvider::SetExtensionOptionValue(string key, const char *env_var_name) {
	const auto env_value = std::getenv(env_var_name);
	if (env_value) {
		if (StringUtil::Lower(env_value) == "false") {
			this->config.SetOption(key, Value(false));
		} else if (StringUtil::Lower(env_value) == "true") {
			this->config.SetOption(key, Value(true));
		} else {
			this->config.SetOption(key, Value(env_value));
		}
	}
}

void AWSEnvironmentCredentialsProvider::SetAll() {
	this->SetExtensionOptionValue("s3_region", DEFAULT_REGION_ENV_VAR);
	this->SetExtensionOptionValue("s3_region", REGION_ENV_VAR);
	this->SetExtensionOptionValue("s3_access_key_id", ACCESS_KEY_ENV_VAR);
	this->SetExtensionOptionValue("s3_secret_access_key", SECRET_KEY_ENV_VAR);
	this->SetExtensionOptionValue("s3_session_token", SESSION_TOKEN_ENV_VAR);
	this->SetExtensionOptionValue("s3_endpoint", DUCKDB_ENDPOINT_ENV_VAR);
	this->SetExtensionOptionValue("s3_use_ssl", DUCKDB_USE_SSL_ENV_VAR);
	this->SetExtensionOptionValue("s3_kms_key_id", DUCKDB_KMS_KEY_ID_ENV_VAR);
	this->SetExtensionOptionValue("s3_requester_pays", DUCKDB_REQUESTER_PAYS_ENV_VAR);
}

S3AuthParams S3AuthParams::ReadFrom(optional_ptr<FileOpener> opener, FileOpenerInfo &info) {

	// Without a FileOpener we can not access settings nor secrets: return empty auth params
	if (!opener) {
		return {};
	}

	const char *secret_types[] = {"s3", "r2", "gcs", "aws"};
	S3KeyValueReader secret_reader(*opener, info, secret_types, 4);

	return ReadFrom(secret_reader, info.file_path);
}

static bool EndpointIsAWS(const string &endpoint) {
	if (endpoint.empty()) {
		// default (empty) endpoint is AWS
		return true;
	}
	if (StringUtil::StartsWith(endpoint, "s3.") && StringUtil::EndsWith(endpoint, ".amazonaws.com")) {
		return true;
	}
	return false;
}

void S3AuthParams::InitializeEndpoint() {
	if (!EndpointIsAWS(endpoint)) {
		return;
	}
	if (region.empty()) {
		if (access_key_id.empty()) {
			// no access key and no region - use legacy global endpoint
			endpoint = "s3.amazonaws.com";
			return;
		}
		// access key but no region - default to us-east-1
		region = "us-east-1";
	}
	endpoint = StringUtil::Format("s3.%s.amazonaws.com", region);
}

S3AuthParams S3AuthParams::ReadFrom(S3KeyValueReader &secret_reader, const string &file_path) {
	auto result = S3AuthParams();

	// These settings we just set or leave to their S3AuthParams default value
	secret_reader.TryGetSecretKeyOrSetting("region", "s3_region", result.region);
	secret_reader.TryGetSecretKeyOrSetting("key_id", "s3_access_key_id", result.access_key_id);
	secret_reader.TryGetSecretKeyOrSetting("secret", "s3_secret_access_key", result.secret_access_key);
	secret_reader.TryGetSecretKeyOrSetting("session_token", "s3_session_token", result.session_token);
	secret_reader.TryGetSecretKeyOrSetting("region", "s3_region", result.region);
	secret_reader.TryGetSecretKeyOrSetting("use_ssl", "s3_use_ssl", result.use_ssl);
	secret_reader.TryGetSecretKeyOrSetting("kms_key_id", "s3_kms_key_id", result.kms_key_id);
	secret_reader.TryGetSecretKeyOrSetting("s3_url_compatibility_mode", "s3_url_compatibility_mode",
	                                       result.s3_url_compatibility_mode);
	secret_reader.TryGetSecretKeyOrSetting("requester_pays", "s3_requester_pays", result.requester_pays);
	// Endpoint and url style are slightly more complex and require special handling for gcs and r2
	auto endpoint_result = secret_reader.TryGetSecretKeyOrSetting("endpoint", "s3_endpoint", result.endpoint);
	auto url_style_result = secret_reader.TryGetSecretKeyOrSetting("url_style", "s3_url_style", result.url_style);

	if (StringUtil::StartsWith(file_path, "gcs://") || StringUtil::StartsWith(file_path, "gs://")) {
		// For GCS urls we force the endpoint and vhost path style, allowing only to be overridden by secrets
		if (result.endpoint.empty() || !endpoint_result || endpoint_result.GetScope() != SettingScope::SECRET) {
			result.endpoint = "storage.googleapis.com";
		}
		if (result.url_style.empty() || !url_style_result || url_style_result.GetScope() != SettingScope::SECRET) {
			result.url_style = "path";
		}
		// Read bearer token for GCS
		secret_reader.TryGetSecretKey("bearer_token", result.oauth2_bearer_token);
	}
	result.InitializeEndpoint();

	return result;
}

void S3AuthParams::SetRegion(string new_region) {
	region = std::move(new_region);
	InitializeEndpoint();
}

S3KeyValueReader::S3KeyValueReader(FileOpener &opener_p, optional_ptr<FileOpenerInfo> info, const char **secret_types,
                                   const idx_t secret_types_len)
    : S3KeyValueReader(KeyValueSecretReader {opener_p, info, secret_types, secret_types_len}) {
}

S3KeyValueReader::S3KeyValueReader(const KeyValueSecretReader &_reader) : reader(_reader) {
	Value use_env_vars_for_secret_info_setting;
	reader.TryGetSecretKeyOrSetting("enable_global_s3_configuration", "enable_global_s3_configuration",
	                                use_env_vars_for_secret_info_setting);
	use_env_variables_for_secret_settings = use_env_vars_for_secret_info_setting.GetValue<bool>();
}

} // namespace duckdb
