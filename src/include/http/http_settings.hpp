#pragma once

namespace duckdb {

class DBConfig;

struct HTTPSettings {
	static void Register(DBConfig &config);
	static void Initialize(DBConfig &config);
};

} // namespace duckdb
