#pragma once

namespace duckdb {

class DBConfig;

struct S3Settings {
	static void Register(DBConfig &config);
	static void Initialize(DBConfig &config);
};

} // namespace duckdb
