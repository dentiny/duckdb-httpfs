#include "s3/s3_list.hpp"

#include "s3/s3fs.hpp"

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/logger.hpp"

#include <map>

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
			if (!completed)
				return true;
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

	parsed_s3_url = S3Url::Parse(glob_pattern, s3_auth_params);
	auto parsed_glob_url = parsed_s3_url.trimmed_s3_url;

	// AWS matches on prefix, not glob pattern, so we take a substring until the first wildcard char for the aws calls
	auto first_wildcard_pos = parsed_glob_url.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		expanded_files.emplace_back(glob_pattern);
		finished = true;
		return;
	}

	shared_path = parsed_glob_url.substr(0, first_wildcard_pos);

	S3Url::ReadQueryParams(parsed_s3_url.query_param, s3_auth_params);
	request_session = S3RequestExecutor::CreateSession(opener, glob_pattern, s3_auth_params);
}

bool S3GlobResult::ExpandNextPath() const {
	if (finished) {
		return false;
	}

	const vector<string> pattern_splits = StringUtil::Split(parsed_s3_url.key, "/");

	vector<OpenFileInfo> s3_keys;
	if (!current_common_prefix.empty()) {
		// we have common prefixes left to scan - perform the request
		auto prefix_path = parsed_s3_url.prefix + parsed_s3_url.bucket + '/' + current_common_prefix;

		current_common_prefix = S3Url::Decode(current_common_prefix);
		vector<string> key_splits = StringUtil::Split(current_common_prefix, "/");
		const bool is_match =
		    Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end(), false);
		if (is_match) {
			prefix_path = S3Url::Decode(prefix_path);
			auto prefix_res = AWSListObjectV2::Request(fs.GetEncryptionUtil(), *request_session, prefix_path,
			                                           common_prefix_continuation_token, true);

			AWSListObjectV2::ParseFileList(prefix_res, s3_keys);
			auto more_prefixes = AWSListObjectV2::ParseCommonPrefix(prefix_res);
			common_prefixes.insert(common_prefixes.end(), more_prefixes.begin(), more_prefixes.end());

			common_prefix_continuation_token = AWSListObjectV2::ParseContinuationToken(prefix_res);
		}

		if (common_prefix_continuation_token.empty()) {
			// we are done with the current common prefix
			// either move on to the next one, or finish up
			if (common_prefixes.empty()) {
				// done - we need to do a top-level request again next
				current_common_prefix = string();
			} else {
				// process the next prefix
				current_common_prefix = common_prefixes.back();
				common_prefixes.pop_back();
			}
		}
	} else {
		if (!common_prefixes.empty()) {
			throw InternalException("We have common prefixes but we are doing a top-level request");
		}

		Value value;
		bool allow_s3_recursive_globbing = true;
		if (FileOpener::TryGetCurrentSetting(opener, "s3_allow_recursive_globbing", value)) {
			allow_s3_recursive_globbing = value.GetValue<bool>();
		}

		const bool investigate_use_recursive_glob = !StringUtil::Contains(parsed_s3_url.key, "**") &&
		                                            allow_s3_recursive_globbing && glob_type == GlobType::UNKNOWN;
		// issue the main request

		bool perform_listing = (glob_type != GlobType::HIERARCHICAL);

		// First perform listing once (default will get back up to 1000 elements)
		string response_str = AWSListObjectV2::Request(fs.GetEncryptionUtil(), *request_session, shared_path,
		                                               main_continuation_token, !perform_listing);

		string next_continuation_token = AWSListObjectV2::ParseContinuationToken(response_str);

		// If we could have used recursive globbing AND there are more files, check average number of files per folder
		if (investigate_use_recursive_glob && !next_continuation_token.empty()) {
			vector<OpenFileInfo> s3_keys_tmp;
			AWSListObjectV2::ParseFileList(response_str, s3_keys_tmp);
			idx_t found = 0;
			unordered_set<string> my_set;
			for (auto &s3_key : s3_keys_tmp) {
				vector<string> key_splits = StringUtil::Split(s3_key.path, "/");

				found++;
				string x = "";
				key_splits.pop_back();
				for (string y : key_splits) {
					x += y + "/";
				}
				my_set.insert(x);
			}

			if (my_set.size() * 100 < found) {
				// We have at least 100 files per folder, this should make so that hierarchical listing price is
				// amortized folder of hierarchical glob

				// Start from scratch:
				// 1. clear keys
				s3_keys_tmp.clear();
				// 2. do request again, now passing true
				response_str = AWSListObjectV2::Request(fs.GetEncryptionUtil(), *request_session, shared_path,
				                                        main_continuation_token, true);

				// 3. now set next_continuation_token
				next_continuation_token = AWSListObjectV2::ParseContinuationToken(response_str);

				glob_type = GlobType::HIERARCHICAL;
			} else {
				glob_type = GlobType::LISTING;
			}
		}

		main_continuation_token = next_continuation_token;
		AWSListObjectV2::ParseFileList(response_str, s3_keys);

		// parse the list of common prefixes
		common_prefixes = AWSListObjectV2::ParseCommonPrefix(response_str);
		if (!common_prefixes.empty()) {
			// we have common prefixes - set one up for the next request
			current_common_prefix = common_prefixes.back();
			common_prefixes.pop_back();
		}
	}

	if (main_continuation_token.empty() && current_common_prefix.empty()) {
		// we are done
		finished = true;
	}

	for (auto &s3_key : s3_keys) {

		vector<string> key_splits = StringUtil::Split(s3_key.path, "/");
		bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end(), true);

		if (is_match) {
			auto result_full_url = parsed_s3_url.prefix + parsed_s3_url.bucket + "/" + s3_key.path;
			// if a ? char was present, we re-add it here as the url parsing will have trimmed it.
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
	return true;
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

// Tolerant variant for error bodies: a truncated/malformed body must not escalate to a DB-invalidating
// InternalException, so an unmatched open tag is "not found".

string AWSListObjectV2::Request(EncryptionUtil &encryption_util, HTTPRequestSession &session, const string &path,
                                const string &continuation_token, bool use_delimiter, optional_idx max_keys) {
	auto create_request_data = [&]() {
		auto captured = session.Capture();
		auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
		auto auth_params = snapshot.auth_params;
		auto parsed_url = S3Url::Parse(path, auth_params);

		map<string, string> request_params;
		if (!continuation_token.empty()) {
			request_params["continuation-token"] = S3Url::Encode(continuation_token, true);
		}
		if (use_delimiter) {
			request_params["delimiter"] = "%2F";
		}
		request_params["encoding-type"] = "url";
		request_params["list-type"] = "2";
		if (max_keys.IsValid()) {
			request_params["max-keys"] = to_string(max_keys.GetIndex());
		}
		request_params["prefix"] = S3Url::Encode(parsed_url.key, true);

		string encoded_params;
		for (const auto &param : request_params) {
			encoded_params += param.first + "=" + param.second + "&";
		}
		encoded_params.pop_back();

		S3RequestData result;
		result.captured = captured;
		result.auth_params = std::move(auth_params);
		result.http_params = snapshot.CreateRequestParams();
		result.source_url = path;
		auto request_path = parsed_url.path.substr(0, parsed_url.path.length() - parsed_url.key.length());
		result.http_url = parsed_url.http_proto + parsed_url.host + S3Url::Encode(request_path) + "?" + encoded_params;
		if (S3Url::IsGCS(path) && !result.auth_params.oauth2_bearer_token.empty()) {
			result.headers["Authorization"] = "Bearer " + result.auth_params.oauth2_bearer_token;
			result.headers["Host"] = parsed_url.host;
		} else {
			result.headers = S3RequestUtil::CreateHeader(encryption_util, request_path, encoded_params, parsed_url.host,
			                                             "s3", "GET", result.auth_params);
		}
		snapshot.AddConfiguredHeaders(result.headers);
		return result;
	};

	auto response = S3RequestExecutor::Run(
	    path, create_request_data, true,
	    [&](S3RequestData &request_data) {
		    auto &params = request_data.http_params->Cast<HTTPFSParams>();
		    GetRequestInfo get_request(request_data.http_url, request_data.headers, params, nullptr, nullptr);
		    return S3RequestExecutor::SendSessionRequest(session, request_data.captured, params, get_request);
	    },
	    [&](const S3RequestData &request_data) { return S3RequestExecutor::TryRefreshSession(session, request_data); },
	    [&](const S3RequestData &request_data, string correct_region) {
		    string previous_region;
		    if (S3RequestExecutor::SetSessionRegion(session, correct_region, previous_region)) {
			    auto &params = request_data.http_params->Cast<HTTPFSParams>();
			    DUCKDB_LOG_WARNING(
			        params.logger,
			        "Ran S3 glob \"%s\" from incorrect region \"%s\" - retrying with updated region \"%s\".\n"
			        "Consider setting the S3 region to this explicitly to avoid extra round-trips.",
			        path, previous_region, correct_region);
		    }
	    });
	if (response->HasRequestError()) {
		throw IOException("%s error for HTTP GET to '%s'", response->GetRequestError(), path);
	}
	if (static_cast<int>(response->status) >= 400) {
		auto captured = session.Capture();
		auto &snapshot = captured.snapshot->Cast<S3RequestSnapshot>();
		string trimmed_path = path;
		StringUtil::RTrim(trimmed_path, "/");
		throw S3RequestUtil::GetError(snapshot.auth_params, *response, trimmed_path);
	}
	return std::move(response->body);
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
