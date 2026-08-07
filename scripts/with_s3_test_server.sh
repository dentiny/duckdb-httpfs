#!/usr/bin/env bash

set -e

# shellcheck source=scripts/run_s3_test_server.sh
source ./scripts/run_s3_test_server.sh
# shellcheck source=scripts/set_s3_test_server_variables.sh
source ./scripts/set_s3_test_server_variables.sh

exec "$@"
