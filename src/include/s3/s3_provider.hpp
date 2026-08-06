#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {

class CreateSecretFunction;
class KeyValueSecret;
class Value;
struct CreateSecretInput;
struct S3AuthParams;
class S3KeyValueReader;

enum class S3ProviderType : uint8_t { S3, GCS, R2 };

enum class S3AuthType : uint8_t { ANONYMOUS, SIGV4, BEARER };

struct S3ProviderMatch {
	S3ProviderType type;
	string prefix;
};

struct S3Provider {
	static const array<const char *, 4> &SecretTypes();
	static const array<const char *, 12> &CredentialMaterialKeys();

	static optional<S3ProviderMatch> TryMatchUrl(const string &url);
	static S3ProviderMatch MatchUrl(const string &url);

	static vector<string> DefaultSecretScope(const string &secret_type);
	static void SetSecretNamedParameters(const string &secret_type, CreateSecretFunction &function);
	static void ApplySecretDefaults(const CreateSecretInput &input, KeyValueSecret &secret);
	static bool TryApplySecretOption(const CreateSecretInput &input, const string &name, const Value &value,
	                                 KeyValueSecret &secret);

	static void ReadAuthParams(S3KeyValueReader &secret_reader, const string &file_path, S3AuthParams &result);
	static void InitializeAuthParams(S3AuthParams &auth_params);
	static S3AuthType GetAuthType(const S3AuthParams &auth_params);
	static string GetBadRequestError(const S3AuthParams &auth_params, const string &correct_region = "");
	static string GetAuthError(const S3AuthParams &auth_params);
};

} // namespace duckdb
