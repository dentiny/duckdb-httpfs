#include "s3/s3_xml_response.hpp"

#include "duckdb/common/pair.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>

namespace duckdb {

namespace {

struct S3XMLQualifiedName {
	string prefix;
	string local_name;
};

struct S3XMLAttribute {
	S3XMLQualifiedName name;
	string value;
};

struct S3XMLNamespaceBinding {
	string prefix;
	string uri;
};

struct S3XMLElementFrame {
	S3XMLQualifiedName name;
	string namespace_uri;
	string text;
	idx_t namespace_count;
	bool has_child_elements = false;
};

struct S3XMLChild {
	string local_name;
	string namespace_uri;
	string text;
	bool has_child_elements;
};

struct S3XMLReader {
	explicit S3XMLReader(const string &input_p) : input(input_p) {
		namespaces.push_back({"xml", XML_NAMESPACE});
	}

	bool Parse(S3XMLResponse &response) {
		response = S3XMLResponse();
		if (!ValidateCharacters()) {
			return false;
		}
		if (HasPrefix(UTF8_BOM)) {
			position += 3;
		}
		if (HasPrefix("<?xml")) {
			if (!ParseXMLDeclaration()) {
				return false;
			}
		}

		while (position < input.size()) {
			if (input[position] != '<') {
				if (!ParseText()) {
					return false;
				}
				continue;
			}
			if (HasPrefix("<!--")) {
				if (!ParseComment()) {
					return false;
				}
				continue;
			}
			if (HasPrefix("<![CDATA[")) {
				if (!ParseCDATA()) {
					return false;
				}
				continue;
			}
			if (HasPrefix("</")) {
				if (!ParseEndElement()) {
					return false;
				}
				continue;
			}
			if (HasPrefix("<!") || HasPrefix("<?")) {
				return false;
			}
			if (!ParseStartElement()) {
				return false;
			}
		}

		if (!root_seen || !root_closed || !elements.empty()) {
			return false;
		}
		InterpretResponse(response);
		return true;
	}

private:
	static constexpr const char *UTF8_BOM = "\xEF\xBB\xBF";
	static constexpr const char *XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace";
	static constexpr const char *XMLNS_NAMESPACE = "http://www.w3.org/2000/xmlns/";
	static constexpr const char *S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/";

	bool HasPrefix(const char *prefix) const {
		return input.compare(position, strlen(prefix), prefix) == 0;
	}

	static bool IsXMLWhitespace(char value) {
		return value == ' ' || value == '\t' || value == '\r' || value == '\n';
	}

	static bool IsNameStart(char value) {
		return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || value == '_';
	}

	static bool IsNameCharacter(char value) {
		return IsNameStart(value) || (value >= '0' && value <= '9') || value == '.' || value == '-';
	}

	static bool IsXMLCharacter(int32_t codepoint) {
		return codepoint == 0x9 || codepoint == 0xA || codepoint == 0xD || (codepoint >= 0x20 && codepoint <= 0xD7FF) ||
		       (codepoint >= 0xE000 && codepoint <= 0xFFFD) || (codepoint >= 0x10000 && codepoint <= 0x10FFFF);
	}

	bool ValidateCharacters() const {
		if (!Utf8Proc::IsValid(input.c_str(), input.size())) {
			return false;
		}
		idx_t codepoint_position = 0;
		while (codepoint_position < input.size()) {
			int codepoint_size;
			auto codepoint = Utf8Proc::UTF8ToCodepoint(input.c_str() + codepoint_position, codepoint_size,
			                                           input.size() - codepoint_position);
			if (!IsXMLCharacter(codepoint)) {
				return false;
			}
			codepoint_position += static_cast<idx_t>(codepoint_size);
		}
		return true;
	}

	void SkipWhitespace() {
		while (position < input.size() && IsXMLWhitespace(input[position])) {
			position++;
		}
	}

	bool ParseQualifiedName(S3XMLQualifiedName &name) {
		name = S3XMLQualifiedName();
		if (position >= input.size() || !IsNameStart(input[position])) {
			return false;
		}
		auto first_begin = position++;
		while (position < input.size() && IsNameCharacter(input[position])) {
			position++;
		}
		auto first = input.substr(first_begin, position - first_begin);
		if (position >= input.size() || input[position] != ':') {
			name.local_name = std::move(first);
			return true;
		}
		name.prefix = std::move(first);
		position++;
		if (position >= input.size() || !IsNameStart(input[position])) {
			return false;
		}
		auto local_begin = position++;
		while (position < input.size() && IsNameCharacter(input[position])) {
			position++;
		}
		name.local_name = input.substr(local_begin, position - local_begin);
		return position >= input.size() || input[position] != ':';
	}

	static bool SameName(const S3XMLQualifiedName &left, const S3XMLQualifiedName &right) {
		return left.prefix == right.prefix && left.local_name == right.local_name;
	}

	bool ParseAttributeValue(string &value) {
		value.clear();
		if (position >= input.size() || (input[position] != '\'' && input[position] != '"')) {
			return false;
		}
		auto quote = input[position++];
		while (position < input.size() && input[position] != quote) {
			if (input[position] == '<') {
				return false;
			}
			if (input[position] == '&') {
				if (!ParseReference(value)) {
					return false;
				}
				continue;
			}
			if (IsXMLWhitespace(input[position])) {
				value.push_back(' ');
			} else {
				value.push_back(input[position]);
			}
			position++;
		}
		if (position >= input.size()) {
			return false;
		}
		position++;
		return true;
	}

	bool ParseDeclarationValue(string &value) {
		value.clear();
		if (position >= input.size() || (input[position] != '\'' && input[position] != '"')) {
			return false;
		}
		auto quote = input[position++];
		while (position < input.size() && input[position] != quote) {
			if (input[position] == '<' || input[position] == '&' || !IsNameCharacter(input[position])) {
				return false;
			}
			value.push_back(input[position++]);
		}
		if (position >= input.size()) {
			return false;
		}
		position++;
		return true;
	}

	bool ParseReference(string &result) {
		if (position >= input.size() || input[position] != '&') {
			return false;
		}
		auto reference_end = input.find(';', position + 1);
		if (reference_end == string::npos) {
			return false;
		}
		auto reference = input.substr(position + 1, reference_end - position - 1);
		position = reference_end + 1;
		if (reference == "lt") {
			result.push_back('<');
			return true;
		}
		if (reference == "gt") {
			result.push_back('>');
			return true;
		}
		if (reference == "amp") {
			result.push_back('&');
			return true;
		}
		if (reference == "apos") {
			result.push_back('\'');
			return true;
		}
		if (reference == "quot") {
			result.push_back('"');
			return true;
		}
		if (reference.empty() || reference[0] != '#') {
			return false;
		}

		idx_t digit_position = 1;
		uint32_t base = 10;
		if (digit_position < reference.size() && reference[digit_position] == 'x') {
			base = 16;
			digit_position++;
		}
		if (digit_position == reference.size()) {
			return false;
		}
		uint32_t codepoint = 0;
		for (; digit_position < reference.size(); digit_position++) {
			auto current = reference[digit_position];
			uint32_t digit;
			if (current >= '0' && current <= '9') {
				digit = static_cast<uint32_t>(current - '0');
			} else if (base == 16 && current >= 'a' && current <= 'f') {
				digit = static_cast<uint32_t>(current - 'a' + 10);
			} else if (base == 16 && current >= 'A' && current <= 'F') {
				digit = static_cast<uint32_t>(current - 'A' + 10);
			} else {
				return false;
			}
			if (digit >= base || codepoint > (0x10FFFFU - digit) / base) {
				return false;
			}
			codepoint = codepoint * base + digit;
		}
		if (!IsXMLCharacter(static_cast<int32_t>(codepoint))) {
			return false;
		}
		char encoded[4];
		int encoded_size;
		if (!Utf8Proc::CodepointToUtf8(static_cast<int>(codepoint), encoded_size, encoded)) {
			return false;
		}
		result.append(encoded, static_cast<idx_t>(encoded_size));
		return true;
	}

	bool ParseXMLDeclaration() {
		position += strlen("<?xml");
		if (position >= input.size() || !IsXMLWhitespace(input[position])) {
			return false;
		}
		vector<S3XMLAttribute> attributes;
		while (true) {
			SkipWhitespace();
			if (HasPrefix("?>")) {
				position += 2;
				break;
			}
			S3XMLAttribute attribute;
			if (!ParseQualifiedName(attribute.name) || !attribute.name.prefix.empty()) {
				return false;
			}
			SkipWhitespace();
			if (position >= input.size() || input[position] != '=') {
				return false;
			}
			position++;
			SkipWhitespace();
			if (!ParseDeclarationValue(attribute.value)) {
				return false;
			}
			attributes.push_back(std::move(attribute));
			if (!HasPrefix("?>") && (position >= input.size() || !IsXMLWhitespace(input[position]))) {
				return false;
			}
		}
		if (attributes.empty() || attributes[0].name.local_name != "version" || attributes[0].value != "1.0") {
			return false;
		}
		idx_t attribute_index = 1;
		if (attribute_index < attributes.size() && attributes[attribute_index].name.local_name == "encoding") {
			auto encoding = attributes[attribute_index++].value;
			std::transform(encoding.begin(), encoding.end(), encoding.begin(),
			               [](unsigned char value) { return static_cast<char>(std::tolower(value)); });
			if (encoding != "utf-8") {
				return false;
			}
		}
		if (attribute_index < attributes.size() && attributes[attribute_index].name.local_name == "standalone") {
			auto standalone = attributes[attribute_index++].value;
			if (standalone != "yes" && standalone != "no") {
				return false;
			}
		}
		return attribute_index == attributes.size();
	}

	bool ParseComment() {
		auto comment_begin = position + strlen("<!--");
		auto comment_end = input.find("-->", comment_begin);
		if (comment_end == string::npos) {
			return false;
		}
		auto invalid = input.find("--", comment_begin);
		if (invalid != comment_end) {
			return false;
		}
		position = comment_end + strlen("-->");
		return true;
	}

	bool ParseCDATA() {
		if (elements.empty()) {
			return false;
		}
		auto content_begin = position + strlen("<![CDATA[");
		auto content_end = input.find("]]>", content_begin);
		if (content_end == string::npos) {
			return false;
		}
		elements.back().text.append(input, content_begin, content_end - content_begin);
		position = content_end + strlen("]]>");
		return true;
	}

	bool ParseText() {
		auto text_end = input.find('<', position);
		if (text_end == string::npos) {
			text_end = input.size();
		}
		if (elements.empty()) {
			for (; position < text_end; position++) {
				if (!IsXMLWhitespace(input[position])) {
					return false;
				}
			}
			return true;
		}
		if (input.find("]]>", position) < text_end) {
			return false;
		}
		while (position < text_end) {
			if (input[position] == '&') {
				if (!ParseReference(elements.back().text) || position > text_end) {
					return false;
				}
			} else {
				elements.back().text.push_back(input[position++]);
			}
		}
		return true;
	}

	bool ParseStartElement() {
		if (root_closed || position >= input.size() || input[position] != '<') {
			return false;
		}
		position++;
		S3XMLQualifiedName element_name;
		if (!ParseQualifiedName(element_name) || element_name.prefix == "xmlns") {
			return false;
		}

		vector<S3XMLAttribute> attributes;
		bool self_closing = false;
		while (true) {
			if (position >= input.size()) {
				return false;
			}
			if (input[position] == '>') {
				position++;
				break;
			}
			if (input[position] == '/' && position + 1 < input.size() && input[position + 1] == '>') {
				position += 2;
				self_closing = true;
				break;
			}
			if (!IsXMLWhitespace(input[position])) {
				return false;
			}
			SkipWhitespace();
			if (position >= input.size() || input[position] == '>' || input[position] == '/') {
				continue;
			}
			S3XMLAttribute attribute;
			if (!ParseQualifiedName(attribute.name)) {
				return false;
			}
			for (const auto &existing : attributes) {
				if (SameName(existing.name, attribute.name)) {
					return false;
				}
			}
			SkipWhitespace();
			if (position >= input.size() || input[position] != '=') {
				return false;
			}
			position++;
			SkipWhitespace();
			if (!ParseAttributeValue(attribute.value)) {
				return false;
			}
			attributes.push_back(std::move(attribute));
		}

		auto namespace_count = namespaces.size();
		if (!ApplyNamespaceDeclarations(attributes)) {
			namespaces.resize(namespace_count);
			return false;
		}
		string namespace_uri;
		if (!ResolveNamespace(element_name, true, namespace_uri) || !ValidateAttributes(attributes)) {
			namespaces.resize(namespace_count);
			return false;
		}
		if (elements.empty()) {
			if (root_seen) {
				namespaces.resize(namespace_count);
				return false;
			}
			root_seen = true;
			root_local_name = element_name.local_name;
			root_namespace_uri = namespace_uri;
		} else {
			elements.back().has_child_elements = true;
		}
		elements.push_back({std::move(element_name), std::move(namespace_uri), string(), namespace_count, false});
		if (self_closing) {
			return CloseElement();
		}
		return true;
	}

	bool ParseEndElement() {
		if (elements.empty()) {
			return false;
		}
		position += 2;
		S3XMLQualifiedName name;
		if (!ParseQualifiedName(name)) {
			return false;
		}
		SkipWhitespace();
		if (position >= input.size() || input[position] != '>') {
			return false;
		}
		position++;
		if (!SameName(elements.back().name, name)) {
			return false;
		}
		return CloseElement();
	}

	bool CloseElement() {
		auto frame = std::move(elements.back());
		elements.pop_back();
		namespaces.resize(frame.namespace_count);
		if (elements.empty()) {
			root_closed = true;
			return true;
		}
		if (elements.size() == 1) {
			children.push_back({frame.name.local_name, std::move(frame.namespace_uri), std::move(frame.text),
			                    frame.has_child_elements});
		}
		return true;
	}

	bool ApplyNamespaceDeclarations(const vector<S3XMLAttribute> &attributes) {
		for (const auto &attribute : attributes) {
			string prefix;
			if (attribute.name.prefix.empty() && attribute.name.local_name == "xmlns") {
				prefix = string();
			} else if (attribute.name.prefix == "xmlns") {
				prefix = attribute.name.local_name;
			} else {
				continue;
			}
			if (prefix == "xmlns" || attribute.value == XMLNS_NAMESPACE) {
				return false;
			}
			if (prefix == "xml") {
				if (attribute.value != XML_NAMESPACE) {
					return false;
				}
			} else if (attribute.value == XML_NAMESPACE || (!prefix.empty() && attribute.value.empty())) {
				return false;
			}
			namespaces.push_back({std::move(prefix), attribute.value});
		}
		return true;
	}

	bool ResolveNamespace(const S3XMLQualifiedName &name, bool element, string &namespace_uri) const {
		if (name.prefix == "xmlns") {
			return false;
		}
		if (name.prefix.empty() && !element) {
			namespace_uri.clear();
			return true;
		}
		for (auto binding = namespaces.rbegin(); binding != namespaces.rend(); binding++) {
			if (binding->prefix == name.prefix) {
				namespace_uri = binding->uri;
				return true;
			}
		}
		if (name.prefix.empty()) {
			namespace_uri.clear();
			return true;
		}
		return false;
	}

	bool ValidateAttributes(const vector<S3XMLAttribute> &attributes) const {
		vector<pair<string, string>> expanded_names;
		for (const auto &attribute : attributes) {
			if ((attribute.name.prefix.empty() && attribute.name.local_name == "xmlns") ||
			    attribute.name.prefix == "xmlns") {
				continue;
			}
			string namespace_uri;
			if (!ResolveNamespace(attribute.name, false, namespace_uri)) {
				return false;
			}
			for (const auto &existing : expanded_names) {
				if (existing.first == namespace_uri && existing.second == attribute.name.local_name) {
					return false;
				}
			}
			expanded_names.emplace_back(std::move(namespace_uri), attribute.name.local_name);
		}
		return true;
	}

	bool HasSupportedNamespace() const {
		return root_namespace_uri.empty() || root_namespace_uri == S3_NAMESPACE;
	}

	bool TryGetChildText(const string &local_name, string &result) const {
		idx_t matches = 0;
		for (const auto &child : children) {
			if (child.local_name != local_name || child.namespace_uri != root_namespace_uri) {
				continue;
			}
			matches++;
			if (child.has_child_elements) {
				return false;
			}
			result = child.text;
		}
		return matches == 1;
	}

	void InterpretResponse(S3XMLResponse &response) const {
		if (!HasSupportedNamespace()) {
			return;
		}
		if (root_local_name == "InitiateMultipartUploadResult") {
			if (TryGetChildText("UploadId", response.upload_id) && !response.upload_id.empty()) {
				response.type = S3XMLResponseType::MULTIPART_INITIALIZATION;
			}
			return;
		}
		if (root_local_name == "CompleteMultipartUploadResult") {
			if (TryGetChildText("ETag", response.etag) && !response.etag.empty()) {
				response.type = S3XMLResponseType::MULTIPART_COMPLETION;
			}
			return;
		}
		if (root_local_name != "Error") {
			return;
		}
		response.type = S3XMLResponseType::ERROR;
		if (!TryGetChildText("Code", response.error_code)) {
			response.error_code.clear();
		}
		if (!TryGetChildText("Message", response.error_message)) {
			response.error_message.clear();
		}
		if (!TryGetChildText("AWSAccessKeyId", response.error_access_key_id)) {
			response.error_access_key_id.clear();
		}
	}

private:
	const string &input;
	idx_t position = 0;
	bool root_seen = false;
	bool root_closed = false;
	string root_local_name;
	string root_namespace_uri;
	vector<S3XMLNamespaceBinding> namespaces;
	vector<S3XMLElementFrame> elements;
	vector<S3XMLChild> children;
};

} // namespace

bool S3XMLResponseParser::TryParse(const string &input, S3XMLResponse &response) {
	return S3XMLReader(input).Parse(response);
}

bool S3XMLResponseParser::TryParseError(const string &input, S3XMLError &error) {
	error = S3XMLError();
	S3XMLResponse response;
	if (!TryParse(input, response) || response.type != S3XMLResponseType::ERROR) {
		return false;
	}
	error.code = std::move(response.error_code);
	error.message = std::move(response.error_message);
	error.access_key_id = std::move(response.error_access_key_id);
	return true;
}

} // namespace duckdb
