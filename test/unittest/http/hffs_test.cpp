#include "catch.hpp"

#include "hffs.hpp"

namespace duckdb {

TEST_CASE("Hugging Face list results parse files and directories", "[httpfs][hffs]") {
	const string input = R"([
		{"type":"file","path":"folder/file.parquet","metadata":{"size":42}},
		{"type":"directory","path":"folder/nested"}
	])";
	vector<string> files;
	vector<string> directories;

	HuggingFaceFileSystem::ParseListResult(input, files, directories);

	REQUIRE(files == vector<string> {"/folder/file.parquet"});
	REQUIRE(directories == vector<string> {"/folder/nested"});
}

TEST_CASE("Hugging Face list results parse escaped path characters", "[httpfs][hffs]") {
	const string input = R"([{"type":"file","path":"folder/a\"b\\c.parquet"}])";
	vector<string> files;
	vector<string> directories;

	HuggingFaceFileSystem::ParseListResult(input, files, directories);

	REQUIRE(files == vector<string> {"/folder/a\"b\\c.parquet"});
	REQUIRE(directories.empty());
}

TEST_CASE("Hugging Face list results reject incomplete entries", "[httpfs][hffs]") {
	const string input = R"([{"path":"folder/file.parquet"}])";
	vector<string> files;
	vector<string> directories;

	REQUIRE_THROWS_AS(HuggingFaceFileSystem::ParseListResult(input, files, directories), IOException);
}

} // namespace duckdb
