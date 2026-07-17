#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

docker compose -f "${SCRIPT_DIR}/minio_s3.yml" -p duckdb-minio down --volumes --remove-orphans

if ! rm -rf /tmp/minio_test_data /tmp/minio_root_data; then
  if ! command -v sudo >/dev/null 2>&1; then
    echo "Could not remove the MinIO data directories and sudo is unavailable" >&2
    exit 1
  fi
  sudo rm -rf /tmp/minio_test_data /tmp/minio_root_data
fi
