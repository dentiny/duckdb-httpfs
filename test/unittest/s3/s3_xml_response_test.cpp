#include "catch.hpp"

#include "s3/s3_xml_response.hpp"

namespace duckdb {

TEST_CASE("S3 XML responses follow text and namespace semantics", "[httpfs][s3][xml]") {
	SECTION("predefined and numeric entities are decoded") {
		const string input = "<InitiateMultipartUploadResult><UploadId>opaque&amp;&#38;&#x1F642;</UploadId>"
		                     "</InitiateMultipartUploadResult>";
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		REQUIRE(response.type == S3XMLResponseType::MULTIPART_INITIALIZATION);
		CHECK(response.upload_id == string("opaque&&") + "\xF0\x9F\x99\x82");
	}
	SECTION("direct UTF-8 text is preserved") {
		const string input = "<InitiateMultipartUploadResult><UploadId>opaque-\xF0\x9F\x99\x82</UploadId>"
		                     "</InitiateMultipartUploadResult>";
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		REQUIRE(response.type == S3XMLResponseType::MULTIPART_INITIALIZATION);
		CHECK(response.upload_id == string("opaque-") + "\xF0\x9F\x99\x82");
	}
	SECTION("namespace prefixes are resolved") {
		const string input = "<s3:InitiateMultipartUploadResult xmlns:s3=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
		                     "<s3:UploadId>opaque-id</s3:UploadId></s3:InitiateMultipartUploadResult>";
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		REQUIRE(response.type == S3XMLResponseType::MULTIPART_INITIALIZATION);
		CHECK(response.upload_id == "opaque-id");
	}
	SECTION("default namespaces are inherited") {
		const string input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
		                     "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
		                     "<ETag>&quot;etag&quot;</ETag></CompleteMultipartUploadResult>";
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		REQUIRE(response.type == S3XMLResponseType::MULTIPART_COMPLETION);
		CHECK(response.etag == "\"etag\"");
	}
	SECTION("comments and CDATA contribute text") {
		const string input = "<Error><!-- ignored --><Code><![CDATA[InternalError]]></Code>"
		                     "<Message>failed<!-- ignored --> safely</Message></Error>";
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		REQUIRE(response.type == S3XMLResponseType::ERROR);
		CHECK(response.error_code == "InternalError");
		CHECK(response.error_message == "failed safely");
	}
	SECTION("a UTF-8 BOM is accepted") {
		const string input = "\xEF\xBB\xBF<CompleteMultipartUploadResult><ETag>etag</ETag>"
		                     "</CompleteMultipartUploadResult>";
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		CHECK(response.type == S3XMLResponseType::MULTIPART_COMPLETION);
	}
}

TEST_CASE("S3 XML responses reject malformed XML", "[httpfs][s3][xml]") {
	for (const auto &input :
	     {"<CompleteMultipartUploadResult invalid><ETag>etag</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult attr=value><ETag>etag</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag>etag & broken</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag>&custom;</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag>&#0;</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag>&#xD800;</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag>&#x110000;</ETag></CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult duplicate=\"a\" duplicate=\"b\"><ETag>etag</ETag>"
	      "</CompleteMultipartUploadResult>",
	      "<s3:CompleteMultipartUploadResult><s3:ETag>etag</s3:ETag></s3:CompleteMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag>etag</CompleteMultipartUploadResult></ETag>",
	      "<CompleteMultipartUploadResult/><OtherRoot/>",
	      "<!DOCTYPE CompleteMultipartUploadResult><CompleteMultipartUploadResult/>",
	      "<?other value?><CompleteMultipartUploadResult/>",
	      "<?xml version=\"1.0\"encoding=\"UTF-8\"?><CompleteMultipartUploadResult/>",
	      "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><CompleteMultipartUploadResult/>"}) {
		S3XMLResponse response;
		CHECK_FALSE(S3XMLResponseParser::TryParse(input, response));
	}

	SECTION("invalid UTF-8 is rejected") {
		string input = "<CompleteMultipartUploadResult><ETag>";
		input.push_back(static_cast<char>(0xFF));
		input += "</ETag></CompleteMultipartUploadResult>";
		S3XMLResponse response;
		CHECK_FALSE(S3XMLResponseParser::TryParse(input, response));
	}
	SECTION("literal forbidden XML characters are rejected") {
		string input = "<CompleteMultipartUploadResult><ETag>";
		input.push_back(static_cast<char>(0x01));
		input += "</ETag></CompleteMultipartUploadResult>";
		S3XMLResponse response;
		CHECK_FALSE(S3XMLResponseParser::TryParse(input, response));
	}
}

TEST_CASE("S3 XML success responses require the expected contract", "[httpfs][s3][xml]") {
	for (const auto &input :
	     {"<s3:InitiateMultipartUploadResult xmlns:s3=\"urn:not-s3\"><s3:UploadId>id</s3:UploadId>"
	      "</s3:InitiateMultipartUploadResult>",
	      "<InitiateMultipartUploadResult><Wrapper><UploadId>nested</UploadId></Wrapper>"
	      "</InitiateMultipartUploadResult>",
	      "<InitiateMultipartUploadResult><UploadId>first</UploadId><UploadId>second</UploadId>"
	      "</InitiateMultipartUploadResult>",
	      "<InitiateMultipartUploadResult><UploadId>before<Nested/>after</UploadId>"
	      "</InitiateMultipartUploadResult>",
	      "<CompleteMultipartUploadResult><ETag/></CompleteMultipartUploadResult>", "<UnexpectedMultipartResponse/>"}) {
		S3XMLResponse response;
		REQUIRE(S3XMLResponseParser::TryParse(input, response));
		CHECK(response.type == S3XMLResponseType::UNKNOWN);
	}
}

TEST_CASE("S3 XML errors are extracted without guessing malformed bodies", "[httpfs][s3][xml]") {
	SECTION("all supported details are decoded") {
		S3XMLError error;
		REQUIRE(S3XMLResponseParser::TryParseError(
		    "<Error><Code>InvalidAccessKeyId</Code><Message>bad &amp; expired</Message>"
		    "<AWSAccessKeyId>redacted-id</AWSAccessKeyId></Error>",
		    error));
		CHECK(error.code == "InvalidAccessKeyId");
		CHECK(error.message == "bad & expired");
		CHECK(error.access_key_id == "redacted-id");
	}
	SECTION("code-only errors remain classifiable") {
		S3XMLError error;
		REQUIRE(S3XMLResponseParser::TryParseError("<Error><Code>RequestTimeout</Code></Error>", error));
		CHECK(error.code == "RequestTimeout");
		CHECK(error.message.empty());
	}
	SECTION("malformed and unrelated XML are not errors") {
		S3XMLError error;
		CHECK_FALSE(S3XMLResponseParser::TryParseError("<Error><Code>RequestTimeout", error));
		CHECK_FALSE(S3XMLResponseParser::TryParseError("<Unexpected/>", error));
	}
}

} // namespace duckdb
