#pragma once

namespace duckdb {

struct DBConfig;

struct S3Settings {
	static void Register(DBConfig &config);
	static void Initialize(DBConfig &config);
};

} // namespace duckdb
