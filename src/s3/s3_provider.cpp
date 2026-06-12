#include "s3/s3_provider.hpp"

#include "s3/s3_auth.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"

#include <algorithm>

namespace duckdb {

static const array<S3ProviderMatch, 6> &ProviderMatches() {
	static const array<S3ProviderMatch, 6> provider_matches = {
	    S3ProviderMatch {S3ProviderType::S3, "s3://"},  S3ProviderMatch {S3ProviderType::S3, "s3a://"},
	    S3ProviderMatch {S3ProviderType::S3, "s3n://"}, S3ProviderMatch {S3ProviderType::GCS, "gcs://"},
	    S3ProviderMatch {S3ProviderType::GCS, "gs://"}, S3ProviderMatch {S3ProviderType::R2, "r2://"}};
	return provider_matches;
}

namespace {
//! Additional URL schemes routed to the S3-compatible filesystem, configured
//! via the 's3_compatible_url_schemes' setting (e.g. "oss://", "cos://").
mutex custom_scheme_lock;
vector<string> custom_url_schemes;
} // namespace

string S3Provider::SetCustomUrlSchemes(const string &schemes_csv) {
	//! Schemes already claimed by built-in filesystems or well-known extensions (e.g. azure); allowing these would
	//! hijack their routing
	static const vector<string> reserved_schemes = {"s3://", "s3a://",   "s3n://",   "gcs://", "gs://",
	                                                "r2://", "http://",  "https://", "hf://",  "file://",
	                                                "az://", "azure://", "abfss://", "abfs://"};
	vector<string> result;
	for (auto &scheme : StringUtil::Split(schemes_csv, ',')) {
		auto trimmed = StringUtil::Lower(scheme);
		StringUtil::Trim(trimmed);
		if (trimmed.empty()) {
			continue;
		}
		// Accept both 'oss' and 'oss://'
		if (!StringUtil::EndsWith(trimmed, "://")) {
			trimmed += "://";
		}
		for (auto &reserved : reserved_schemes) {
			if (trimmed == reserved) {
				throw InvalidInputException("Scheme '%s' is already handled by a built-in filesystem and cannot be "
				                            "added to 's3_compatible_url_schemes'",
				                            trimmed);
			}
		}
		if (std::find(result.begin(), result.end(), trimmed) != result.end()) {
			continue;
		}
		result.push_back(std::move(trimmed));
	}
	auto normalized = StringUtil::Join(result, ",");
	lock_guard<mutex> guard(custom_scheme_lock);
	custom_url_schemes = std::move(result);
	return normalized;
}

vector<string> S3Provider::GetCustomUrlSchemes() {
	lock_guard<mutex> guard(custom_scheme_lock);
	return custom_url_schemes;
}

const array<const char *, 4> &S3Provider::SecretTypes() {
	static const array<const char *, 4> secret_types = {"s3", "r2", "gcs", "aws"};
	return secret_types;
}

const array<const char *, 12> &S3Provider::CredentialMaterialKeys() {
	static const array<const char *, 12> credential_material_keys = {
	    "key_id",    "secret",  "session_token",          "region",         "endpoint",     "kms_key_id",
	    "url_style", "use_ssl", "url_compatibility_mode", "requester_pays", "bearer_token", "account_id"};
	return credential_material_keys;
}

optional<S3ProviderMatch> S3Provider::TryMatchUrl(const string &url) {
	auto lower_url = StringUtil::Lower(url);
	for (const auto &provider_match : ProviderMatches()) {
		if (StringUtil::StartsWith(lower_url, provider_match.prefix)) {
			return provider_match;
		}
	}
	// Custom schemes registered via 's3_compatible_url_schemes' are served by the plain S3 provider
	for (auto &prefix : GetCustomUrlSchemes()) {
		if (StringUtil::StartsWith(lower_url, prefix)) {
			return S3ProviderMatch {S3ProviderType::S3, prefix};
		}
	}
	return {};
}

S3ProviderMatch S3Provider::MatchUrl(const string &url) {
	auto provider_match = TryMatchUrl(url);
	if (!provider_match) {
		vector<string> prefixes;
		for (const auto &entry : ProviderMatches()) {
			prefixes.push_back(entry.prefix);
		}
		throw IOException("URL needs to start with %s (or a scheme listed in the 's3_compatible_url_schemes' setting)",
		                  StringUtil::Join(prefixes, ", "));
	}
	return *provider_match;
}

vector<string> S3Provider::DefaultSecretScope(const string &secret_type) {
	if (secret_type == "s3") {
		vector<string> scope {"s3://", "s3n://", "s3a://"};
		for (auto &scheme : GetCustomUrlSchemes()) {
			scope.push_back(scheme);
		}
		return scope;
	}
	if (secret_type == "r2") {
		return {"r2://"};
	}
	if (secret_type == "gcs") {
		return {"gcs://", "gs://"};
	}
	if (secret_type == "aws") {
		return {""};
	}
	throw InternalException("Unknown secret type found in httpfs extension: '%s'", secret_type);
}

void S3Provider::SetSecretNamedParameters(const string &secret_type, CreateSecretFunction &function) {
	if (secret_type == "r2") {
		function.named_parameters["account_id"] = LogicalType::VARCHAR;
	} else if (secret_type == "gcs") {
		function.named_parameters["bearer_token"] = LogicalType::VARCHAR;
	}
}

void S3Provider::ApplySecretDefaults(const CreateSecretInput &input, KeyValueSecret &secret) {
	if (input.type != "r2") {
		return;
	}
	auto account_id = input.options.find("account_id");
	if (account_id == input.options.end()) {
		return;
	}
	secret.secret_map["endpoint"] = account_id->second.ToString() + ".r2.cloudflarestorage.com";
	secret.secret_map["url_style"] = "path";
}

bool S3Provider::TryApplySecretOption(const CreateSecretInput &input, const string &name, const Value &value,
                                      KeyValueSecret &secret) {
	if (name == "account_id" && input.type == "r2") {
		return true;
	}
	if (name == "bearer_token" && input.type == "gcs") {
		secret.secret_map["bearer_token"] = value.ToString();
		secret.redact_keys.insert("bearer_token");
		return true;
	}
	return false;
}

void S3Provider::ReadAuthParams(S3KeyValueReader &secret_reader, const string &file_path, S3AuthParams &result) {
	result.provider_type = MatchUrl(file_path).type;
	secret_reader.TryGetSecretKeyOrSetting("region", "s3_region", result.region);
	secret_reader.TryGetSecretKeyOrSetting("key_id", "s3_access_key_id", result.access_key_id);
	secret_reader.TryGetSecretKeyOrSetting("secret", "s3_secret_access_key", result.secret_access_key);
	secret_reader.TryGetSecretKeyOrSetting("session_token", "s3_session_token", result.session_token);
	secret_reader.TryGetSecretKeyOrSetting("use_ssl", "s3_use_ssl", result.use_ssl);
	secret_reader.TryGetSecretKeyOrSetting("kms_key_id", "s3_kms_key_id", result.kms_key_id);
	secret_reader.TryGetSecretKeysOrSetting("url_compatibility_mode", "s3_url_compatibility_mode",
	                                        "s3_url_compatibility_mode", result.s3_url_compatibility_mode);
	secret_reader.TryGetSecretKeyOrSetting("requester_pays", "s3_requester_pays", result.requester_pays);

	auto endpoint_result = secret_reader.TryGetSecretKeyOrSetting("endpoint", "s3_endpoint", result.endpoint);
	auto url_style_result = secret_reader.TryGetSecretKeyOrSetting("url_style", "s3_url_style", result.url_style);
	if (result.provider_type == S3ProviderType::GCS) {
		if (result.endpoint.empty() || !endpoint_result || endpoint_result.GetScope() != SettingScope::SECRET) {
			result.endpoint = "storage.googleapis.com";
		}
		if (result.url_style.empty() || !url_style_result || url_style_result.GetScope() != SettingScope::SECRET) {
			result.url_style = "path";
		}
		secret_reader.TryGetSecretKey("bearer_token", result.oauth2_bearer_token);
	}
	InitializeAuthParams(result);
}

static bool EndpointIsAWS(const string &endpoint) {
	if (endpoint.empty()) {
		return true;
	}
	return StringUtil::StartsWith(endpoint, "s3.") && StringUtil::EndsWith(endpoint, ".amazonaws.com");
}

void S3Provider::InitializeAuthParams(S3AuthParams &auth_params) {
	if (auth_params.provider_type == S3ProviderType::GCS) {
		if (auth_params.endpoint.empty()) {
			auth_params.endpoint = "storage.googleapis.com";
		}
		if (auth_params.url_style.empty()) {
			auth_params.url_style = "path";
		}
	} else if (auth_params.provider_type == S3ProviderType::R2 && auth_params.endpoint.empty()) {
		throw IOException("R2 requires an endpoint; provide account_id in the secret or s3_endpoint in the URL");
	}
	if (!EndpointIsAWS(auth_params.endpoint)) {
		return;
	}
	if (auth_params.region.empty()) {
		if (auth_params.access_key_id.empty()) {
			auth_params.endpoint = "s3.amazonaws.com";
			return;
		}
		auth_params.region = "us-east-1";
	}
	auth_params.endpoint = StringUtil::Format("s3.%s.amazonaws.com", auth_params.region);
}

S3AuthType S3Provider::GetAuthType(const S3AuthParams &auth_params) {
	if (auth_params.provider_type == S3ProviderType::GCS && !auth_params.oauth2_bearer_token.empty()) {
		return S3AuthType::BEARER;
	}
	if (auth_params.secret_access_key.empty() && auth_params.access_key_id.empty()) {
		return S3AuthType::ANONYMOUS;
	}
	return S3AuthType::SIGV4;
}

string S3Provider::GetBadRequestError(const S3AuthParams &auth_params, const string &correct_region) {
	if (auth_params.provider_type != S3ProviderType::S3) {
		return string();
	}
	string extra_text = "\n\nBad Request - this can be caused by the S3 region being set incorrectly.";
	if (auth_params.region.empty()) {
		extra_text += "\n* No region is provided.";
	} else {
		extra_text += "\n* Provided region is: \"" + auth_params.region + "\"";
	}
	if (!correct_region.empty()) {
		extra_text += "\n* Correct region is: \"" + correct_region + "\"";
	}
	return extra_text;
}

string S3Provider::GetAuthError(const S3AuthParams &auth_params) {
	if (auth_params.provider_type == S3ProviderType::GCS) {
		string extra_text = "\n\nAuthentication Failure - GCS authentication failed.";
		if (auth_params.oauth2_bearer_token.empty() && auth_params.secret_access_key.empty() &&
		    auth_params.access_key_id.empty()) {
			extra_text += "\n* No credentials provided.";
			extra_text += "\n* For OAuth2: CREATE SECRET (TYPE GCS, bearer_token 'your-token')";
			extra_text += "\n* For HMAC: CREATE SECRET (TYPE GCS, key_id 'key', secret 'secret')";
		} else if (!auth_params.oauth2_bearer_token.empty()) {
			extra_text += "\n* Bearer token was provided but authentication failed.";
			extra_text += "\n* Ensure your OAuth2 token is valid and not expired.";
		} else {
			extra_text += "\n* HMAC credentials were provided but authentication failed.";
			extra_text += "\n* Ensure your HMAC key_id and secret are correct.";
		}
		return extra_text;
	}
	if (auth_params.provider_type == S3ProviderType::R2) {
		string extra_text = "\n\nAuthentication Failure - R2 authentication failed.";
		if (auth_params.secret_access_key.empty() && auth_params.access_key_id.empty()) {
			extra_text += "\n* No credentials are provided.";
			extra_text += "\n* Create an R2 secret with account_id, key_id, and secret.";
		} else {
			extra_text += "\n* Credentials were provided, but they did not work.";
		}
		return extra_text;
	}

	string extra_text = "\n\nAuthentication Failure - this is usually caused by invalid or missing credentials.";
	if (auth_params.secret_access_key.empty() && auth_params.access_key_id.empty()) {
		extra_text += "\n* No credentials are provided.";
	} else {
		extra_text += "\n* Credentials are provided, but they did not work.";
	}
	extra_text += "\n* See https://duckdb.org/docs/stable/extensions/httpfs/s3api.html";
	return extra_text;
}

} // namespace duckdb
