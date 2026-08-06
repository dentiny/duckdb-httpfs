#include "s3/s3_list.hpp"

#include "s3/s3fs.hpp"

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end, bool completed) {

	if (key == key_end && !completed) {
		return true;
	}

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			pattern++;
			while (key != key_end) {
				if (Match(key, key_end, pattern, pattern_end, completed)) {
					return true;
				}
				key++;
			}
			if (!completed) {
				return true;
			}
			return false;
		}
		if (!Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	if (pattern != pattern_end && !completed) {
		return true;
	}
	return key == key_end && pattern == pattern_end;
}

enum GlobType { HIERARCHICAL, LISTING, UNKNOWN };

struct S3GlobResult : public LazyMultiFileList {
public:
	S3GlobResult(S3FileSystem &fs_p, const string &path, optional_ptr<FileOpener> opener);

protected:
	bool ExpandNextPath() const override;

private:
	void ScanCurrentCommonPrefix(vector<OpenFileInfo> &s3_keys) const;
	void ScanTopLevel(vector<OpenFileInfo> &s3_keys) const;
	bool ShouldInvestigateRecursiveGlob() const;
	void SelectGlobType(string &response, string &continuation_token) const;
	static bool ContainsDenseDirectories(const vector<OpenFileInfo> &s3_keys);
	void SelectNextCommonPrefix() const;
	void AppendMatchingFiles(vector<OpenFileInfo> &s3_keys) const;

	S3FileSystem &fs;
	string glob_pattern;
	optional_ptr<FileOpener> opener;
	mutable bool finished = false;
	shared_ptr<HTTPRequestSession> request_session;
	string shared_path;
	ParsedS3Url parsed_s3_url;
	mutable string main_continuation_token;
	mutable string current_common_prefix;
	mutable string common_prefix_continuation_token;
	mutable vector<string> common_prefixes;
	mutable GlobType glob_type {UNKNOWN};
};

S3GlobResult::S3GlobResult(S3FileSystem &fs_p, const string &glob_pattern_p, optional_ptr<FileOpener> opener)
    : LazyMultiFileList(FileOpener::TryGetClientContext(opener)), fs(fs_p), glob_pattern(glob_pattern_p),
      opener(opener) {
	if (!opener) {
		throw InternalException("Cannot S3 Glob without FileOpener");
	}
	FileOpenerInfo info = {glob_pattern};

	// Trim any query parameters from the string
	auto s3_auth_params = S3AuthParams::ReadFrom(opener, info);

	// In url compatibility mode, we ignore globs allowing users to query files with the glob chars
	if (s3_auth_params.s3_url_compatibility_mode) {
		expanded_files.emplace_back(glob_pattern);
		finished = true;
		return;
	}

	parsed_s3_url = S3Url::Resolve(glob_pattern, s3_auth_params);
	auto parsed_glob_url = parsed_s3_url.trimmed_s3_url;

	// AWS matches on prefix, not glob pattern, so we take a substring until the first wildcard char for the aws calls
	auto first_wildcard_pos = parsed_glob_url.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		expanded_files.emplace_back(glob_pattern);
		finished = true;
		return;
	}

	shared_path = parsed_glob_url.substr(0, first_wildcard_pos);

	request_session = S3RequestExecutor::CreateSession(opener, glob_pattern, s3_auth_params);
}

bool S3GlobResult::ExpandNextPath() const {
	if (finished) {
		return false;
	}

	vector<OpenFileInfo> s3_keys;
	if (!current_common_prefix.empty()) {
		ScanCurrentCommonPrefix(s3_keys);
	} else {
		ScanTopLevel(s3_keys);
	}

	if (main_continuation_token.empty() && current_common_prefix.empty()) {
		finished = true;
	}
	AppendMatchingFiles(s3_keys);
	return true;
}

void S3GlobResult::ScanCurrentCommonPrefix(vector<OpenFileInfo> &s3_keys) const {
	auto prefix_path = parsed_s3_url.prefix + parsed_s3_url.bucket + '/' + current_common_prefix;
	current_common_prefix = S3Url::Decode(current_common_prefix);
	auto key_splits = StringUtil::Split(current_common_prefix, "/");
	auto pattern_splits = StringUtil::Split(parsed_s3_url.key, "/");
	if (Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end(), false)) {
		prefix_path = S3Url::Decode(prefix_path);
		auto response = AWSListObjectV2::Request(fs.GetEncryptionUtil(), *request_session, prefix_path,
		                                         common_prefix_continuation_token, true);
		AWSListObjectV2::ParseFileList(response, s3_keys);
		auto more_prefixes = AWSListObjectV2::ParseCommonPrefix(response);
		common_prefixes.insert(common_prefixes.end(), more_prefixes.begin(), more_prefixes.end());
		common_prefix_continuation_token = AWSListObjectV2::ParseContinuationToken(response);
	}
	if (common_prefix_continuation_token.empty()) {
		SelectNextCommonPrefix();
	}
}

void S3GlobResult::ScanTopLevel(vector<OpenFileInfo> &s3_keys) const {
	if (!common_prefixes.empty()) {
		throw InternalException("We have common prefixes but we are doing a top-level request");
	}
	const bool hierarchical = glob_type == GlobType::HIERARCHICAL;
	auto response = AWSListObjectV2::Request(fs.GetEncryptionUtil(), *request_session, shared_path,
	                                         main_continuation_token, hierarchical);
	auto continuation_token = AWSListObjectV2::ParseContinuationToken(response);
	if (ShouldInvestigateRecursiveGlob() && !continuation_token.empty()) {
		SelectGlobType(response, continuation_token);
	}
	main_continuation_token = continuation_token;
	AWSListObjectV2::ParseFileList(response, s3_keys);
	common_prefixes = AWSListObjectV2::ParseCommonPrefix(response);
	SelectNextCommonPrefix();
}

bool S3GlobResult::ShouldInvestigateRecursiveGlob() const {
	if (glob_type != GlobType::UNKNOWN || StringUtil::Contains(parsed_s3_url.key, "**")) {
		return false;
	}
	Value value;
	if (!FileOpener::TryGetCurrentSetting(opener, "s3_allow_recursive_globbing", value)) {
		return true;
	}
	return value.GetValue<bool>();
}

void S3GlobResult::SelectGlobType(string &response, string &continuation_token) const {
	vector<OpenFileInfo> s3_keys;
	AWSListObjectV2::ParseFileList(response, s3_keys);
	if (!ContainsDenseDirectories(s3_keys)) {
		glob_type = GlobType::LISTING;
		return;
	}
	response =
	    AWSListObjectV2::Request(fs.GetEncryptionUtil(), *request_session, shared_path, main_continuation_token, true);
	continuation_token = AWSListObjectV2::ParseContinuationToken(response);
	glob_type = GlobType::HIERARCHICAL;
}

bool S3GlobResult::ContainsDenseDirectories(const vector<OpenFileInfo> &s3_keys) {
	unordered_set<string> directories;
	for (const auto &s3_key : s3_keys) {
		auto key_splits = StringUtil::Split(s3_key.path, "/");
		key_splits.pop_back();
		string directory;
		for (const auto &split : key_splits) {
			directory += split + "/";
		}
		directories.insert(std::move(directory));
	}
	return directories.size() * 100 < s3_keys.size();
}

void S3GlobResult::SelectNextCommonPrefix() const {
	if (common_prefixes.empty()) {
		current_common_prefix.clear();
		return;
	}
	current_common_prefix = common_prefixes.back();
	common_prefixes.pop_back();
}

void S3GlobResult::AppendMatchingFiles(vector<OpenFileInfo> &s3_keys) const {
	auto pattern_splits = StringUtil::Split(parsed_s3_url.key, "/");
	for (auto &s3_key : s3_keys) {
		auto key_splits = StringUtil::Split(s3_key.path, "/");
		if (Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end(), true)) {
			auto result_full_url = parsed_s3_url.prefix + parsed_s3_url.bucket + "/" + s3_key.path;
			if (!parsed_s3_url.query_param.empty()) {
				result_full_url += '?' + parsed_s3_url.query_param;
			}
			s3_key.path = std::move(result_full_url);
			auto captured = request_session->Capture();
			auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
			if (!snapshot.auth_params.region.empty()) {
				s3_key.extended_info->options["s3_region"] = snapshot.auth_params.region;
			}
			expanded_files.push_back(std::move(s3_key));
		}
	}
}

unique_ptr<MultiFileList> S3FileSystem::GlobFilesExtended(const string &path, const FileGlobInput &input,
                                                          optional_ptr<FileOpener> opener) {
	return make_uniq<S3GlobResult>(*this, path, opener);
}

bool S3FileSystem::ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
                                     optional_ptr<FileOpener> opener) {
	string trimmed_dir = directory;
	auto sep = PathSeparator(trimmed_dir);
	StringUtil::RTrim(trimmed_dir, sep);
	auto glob_res = GlobFilesExtended(JoinPath(trimmed_dir, "**"), FileGlobOptions::ALLOW_EMPTY, opener);

	if (!glob_res || glob_res->GetExpandResult() == FileExpandResult::NO_FILES) {
		return false;
	}
	auto base_path = trimmed_dir + sep;

	for (auto file : glob_res->Files()) {
		if (!StringUtil::StartsWith(file.path, base_path)) {
			throw InvalidInputException(
			    "Globbed directory \"%s\", but found file \"%s\" that does not start with base path \"%s\"", directory,
			    file.path, base_path);
		}
		file.path = file.path.substr(base_path.size());
		callback(file);
	}

	return true;
}

struct S3ListRequest {
	static string Finish(HTTPRequestSession &session, const string &path, unique_ptr<HTTPResponse> response) {
		if (response->HasRequestError()) {
			throw IOException("%s error for HTTP GET to '%s'", response->GetRequestError(), path);
		}
		if (static_cast<int>(response->status) >= 400) {
			auto captured = session.Capture();
			auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
			auto trimmed_path = path;
			StringUtil::RTrim(trimmed_path, "/");
			throw S3RequestUtil::GetError(snapshot.auth_params, *response, trimmed_path);
		}
		return std::move(response->body);
	}

	static S3RequestQuery BuildQuery(const ParsedS3Url &parsed_url, const string &continuation_token,
	                                 bool use_delimiter, optional_idx max_keys) {
		vector<pair<string, string>> request_params;
		if (!continuation_token.empty()) {
			request_params.emplace_back("continuation-token", continuation_token);
		}
		if (use_delimiter) {
			request_params.emplace_back("delimiter", "/");
		}
		request_params.emplace_back("encoding-type", "url");
		request_params.emplace_back("list-type", "2");
		if (max_keys.IsValid()) {
			request_params.emplace_back("max-keys", to_string(max_keys.GetIndex()));
		}
		request_params.emplace_back("prefix", parsed_url.key);
		return S3RequestQuery(std::move(request_params));
	}
};

string AWSListObjectV2::Request(EncryptionUtil &encryption_util, HTTPRequestSession &session, const string &path,
                                const string &continuation_token, bool use_delimiter, optional_idx max_keys) {
	auto response = S3RequestExecutor::RunSession(
	    encryption_util, session, path, RequestType::GET_REQUEST, S3RequestTarget::BUCKET,
	    [&](const ParsedS3Url &parsed_url) {
		    return S3ListRequest::BuildQuery(parsed_url, continuation_token, use_delimiter, max_keys);
	    },
	    "", "", "",
	    [&](S3RequestData &request_data) {
		    auto &params = request_data.http_params->Cast<HTTPFSParams>();
		    GetRequestInfo get_request(request_data.http_url, request_data.headers, params, nullptr, nullptr);
		    return S3RequestExecutor::SendSessionRequest(session, request_data.captured, params, get_request);
	    },
	    [&](const S3RequestData &request_data, const string &previous_region, const string &correct_region) {
		    auto &params = request_data.http_params->Cast<HTTPFSParams>();
		    DUCKDB_LOG_WARNING(
		        params.logger,
		        "Ran S3 glob \"%s\" from incorrect region \"%s\" - retrying with updated region \"%s\".\n"
		        "Consider setting the S3 region to this explicitly to avoid extra round-trips.",
		        path, previous_region, correct_region);
	    });
	return S3ListRequest::Finish(session, path, std::move(response));
}

void AWSListObjectV2::ParseFileList(string &aws_response, vector<OpenFileInfo> &result) {
	// Example S3 response:
	//	<Contents>
	//		<Key>lineitem_sf10_partitioned_shipdate/l_shipdate%3D1997-03-28/data_0.parquet</Key>
	//		<LastModified>2024-11-09T11:38:08.000Z</LastModified>
	//		<ETag>&quot;bdf10f525f8355fb80d1ff2d8c62cc8b&quot;</ETag>
	//		<Size>1127863</Size>
	//		<StorageClass>STANDARD</StorageClass>
	//	</Contents>
	idx_t cur_pos = 0;
	while (true) {
		string contents;
		auto next_pos = S3RequestUtil::FindTagContents(aws_response, "Contents", cur_pos, contents);
		if (!next_pos.IsValid()) {
			// exhausted all contents
			break;
		}
		// move to the next position
		cur_pos = next_pos.GetIndex();

		// parse the contents
		string key;
		auto key_pos = S3RequestUtil::FindTagContents(contents, "Key", 0, key);
		if (!key_pos.IsValid()) {
			throw InternalException("Key not found in S3 response: %s", contents);
		}
		auto parsed_path = S3Url::Decode(key);
		if (parsed_path.back() == '/') {
			// not a file but a directory
			continue;
		}
		// construct the file
		OpenFileInfo result_file(parsed_path);

		auto extra_info = make_shared_ptr<ExtendedOpenFileInfo>();
		// get file attributes
		string last_modified, etag, size;
		auto last_modified_pos = S3RequestUtil::FindTagContents(contents, "LastModified", 0, last_modified);
		if (last_modified_pos.IsValid()) {
			extra_info->options["last_modified"] = Value(last_modified).DefaultCastAs(LogicalType::TIMESTAMP);
		}
		auto etag_pos = S3RequestUtil::FindTagContents(contents, "ETag", 0, etag);
		if (etag_pos.IsValid()) {
			etag = StringUtil::Replace(etag, "&quot;", "\"");
			etag = StringUtil::Replace(etag, "&#34;", "\"");
			extra_info->options["etag"] = Value(std::move(etag));
		}
		auto size_pos = S3RequestUtil::FindTagContents(contents, "Size", 0, size);
		if (size_pos.IsValid()) {
			extra_info->options["file_size"] = Value(size).DefaultCastAs(LogicalType::UBIGINT);
		}
		result_file.extended_info = std::move(extra_info);
		result.push_back(std::move(result_file));
	}
}

string AWSListObjectV2::ParseContinuationToken(string &aws_response) {

	auto open_tag_pos = aws_response.find("<NextContinuationToken>");
	if (open_tag_pos == string::npos) {
		return "";
	} else {
		auto close_tag_pos = aws_response.find("</NextContinuationToken>", open_tag_pos + 23);
		if (close_tag_pos == string::npos) {
			throw InternalException("Failed to parse S3 result");
		}
		return aws_response.substr(open_tag_pos + 23, close_tag_pos - open_tag_pos - 23);
	}
}

vector<string> AWSListObjectV2::ParseCommonPrefix(string &aws_response) {
	vector<string> s3_prefixes;
	idx_t cur_pos = 0;
	while (true) {
		cur_pos = aws_response.find("<CommonPrefixes>", cur_pos);
		if (cur_pos == string::npos) {
			break;
		}
		auto next_open_tag_pos = aws_response.find("<Prefix>", cur_pos);
		if (next_open_tag_pos == string::npos) {
			throw InternalException("Parsing error while parsing s3 listobject result");
		} else {
			auto next_close_tag_pos = aws_response.find("</Prefix>", next_open_tag_pos + 8);
			if (next_close_tag_pos == string::npos) {
				throw InternalException("Failed to parse S3 result");
			}
			auto parsed_path = aws_response.substr(next_open_tag_pos + 8, next_close_tag_pos - next_open_tag_pos - 8);
			s3_prefixes.push_back(parsed_path);
			cur_pos = next_close_tag_pos + 6;
		}
	}
	return s3_prefixes;
}

} // namespace duckdb
