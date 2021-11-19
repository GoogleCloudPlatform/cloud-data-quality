#!/bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$ROOT/scripts/common.sh"

RUN_DATAPLEX=false
if [[ $# -eq 0 ]]; then
  echo "RUN_DATAPLEX: ${RUN_DATAPLEX}"
else
  if [[ "$1" == "--run-dataplex" || "$1" == "test_dataplex"* ]]; then
    RUN_DATAPLEX=true
    echo "Running Dataplex integration tests..."
  else
    echo "Skipping Dataplex integration tests..."
  fi
fi

# Check that all required env var are set
# if running locally you'd have to ensure the following are correctly set for your project/auth details
require_env_var GOOGLE_CLOUD_PROJECT "Set $GOOGLE_CLOUD_PROJECT to the project_id used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_DATASET "Set $CLOUDDQ_BIGQUERY_DATASET to the BigQuery dataset used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_REGION "Set $CLOUDDQ_BIGQUERY_REGION to the BigQuery region used for integration testing."
if [[ $RUN_DATAPLEX = true ]]; then
  RUN_DATAPLEX=true
  echo "Checking for environment variables required for Dataplex integration tests..."
  require_env_var GCS_BUCKET_NAME "Set $GCS_BUCKET_NAME to the GCS bucket name for staging CloudDQ artifacts and configs."
  require_env_var DATAPLEX_LAKE_NAME "Set $DATAPLEX_LAKE_NAME to the Dataplex Lake used for testing."
  require_env_var DATAPLEX_REGION_ID "Set DATAPLEX_REGION_ID to the region id of the Dataplex Lake. This should be the same as $CLOUDDQ_BIGQUERY_REGION."
  require_env_var DATAPLEX_TARGET_BQ_DATASET "Set $DATAPLEX_TARGET_BQ_DATASET to the Target BQ Dataset used for testing. CloudDQ run fails if the dataset does not exist."
  require_env_var DATAPLEX_TARGET_BQ_TABLE "Set $DATAPLEX_TARGET_BQ_TABLE to the Target BQ Table used for testing. The table will be created in $DATAPLEX_TARGET_BQ_DATASET if not already exist."
  require_env_var DATAPLEX_TASK_SA "Set $DATAPLEX_TASK_SA to the service account used for running Dataplex Tasks in testing."
  require_env_var DATAPLEX_ZONE_ID "Set $DATAPLEX_ZONE_ID to the Dataplex Zone id for testing."
  require_env_var DATAPLEX_BUCKET_NAME "Set $DATAPLEX_BUCKET_NAME to the bucket name for GCS assets in Dataplex Lake used for testing."
  require_env_var DATAPLEX_BIGQUERY_DATASET_ID "Set $DATAPLEX_BIGQUERY_DATASET_ID to the bigquery assets dataset id in Dataplex Lake used for testing."
fi

function bazel_test() {
  set -x
  if [[ -f "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
    bin/bazelisk test \
      --test_env GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}" \
      --test_env GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}" \
      --test_env GOOGLE_SDK_CREDENTIALS="${GOOGLE_SDK_CREDENTIALS:-}" \
      --test_env CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" \
      --test_env CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}" \
      --test_env IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT:-}" \
      --test_env GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-}" \
      --test_env GCS_CLOUDDQ_EXECUTABLE_PATH="${GCS_CLOUDDQ_EXECUTABLE_PATH:-}" \
      --test_env DATAPLEX_LAKE_NAME="${DATAPLEX_LAKE_NAME:-}" \
      --test_env DATAPLEX_REGION_ID="${DATAPLEX_REGION_ID:-}" \
      --test_env DATAPLEX_ENDPOINT="${DATAPLEX_ENDPOINT:-}" \
      --test_env DATAPLEX_TARGET_BQ_DATASET="${DATAPLEX_TARGET_BQ_DATASET:-}" \
      --test_env DATAPLEX_TARGET_BQ_TABLE="${DATAPLEX_TARGET_BQ_TABLE:-}" \
      --test_env DATAPLEX_TASK_SA="${DATAPLEX_TASK_SA:-}" \
      --test_env DATAPLEX_ZONE_ID="${DATAPLEX_ZONE_ID:-}}" \
      --test_env DATAPLEX_BUCKET_NAME="${DATAPLEX_BUCKET_NAME:-}}" \
      --test_env DATAPLEX_BIGQUERY_DATASET_ID="${DATAPLEX_BIGQUERY_DATASET_ID:-}}" \
      //tests"${1:-/...}" ${2+"$2"}
  else
    bin/bazelisk test \
      --test_env GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}" \
      --test_env GOOGLE_SDK_CREDENTIALS="${GOOGLE_SDK_CREDENTIALS:-}" \
      --test_env CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" \
      --test_env CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}" \
      --test_env IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT:-}" \
      --test_env GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-}" \
      --test_env GCS_CLOUDDQ_EXECUTABLE_PATH="${GCS_CLOUDDQ_EXECUTABLE_PATH:-}" \
      --test_env DATAPLEX_LAKE_NAME="${DATAPLEX_LAKE_NAME:-}" \
      --test_env DATAPLEX_REGION_ID="${DATAPLEX_REGION_ID:-}" \
      --test_env DATAPLEX_ENDPOINT="${DATAPLEX_ENDPOINT:-}" \
      --test_env DATAPLEX_TARGET_BQ_DATASET="${DATAPLEX_TARGET_BQ_DATASET:-}" \
      --test_env DATAPLEX_TARGET_BQ_TABLE="${DATAPLEX_TARGET_BQ_TABLE:-}" \
      --test_env DATAPLEX_TASK_SA="${DATAPLEX_TASK_SA:-}" \
      --test_env DATAPLEX_ZONE_ID="${DATAPLEX_ZONE_ID:-}}" \
      --test_env DATAPLEX_BUCKET_NAME="${DATAPLEX_BUCKET_NAME:-}}" \
      --test_env DATAPLEX_BIGQUERY_DATASET_ID="${DATAPLEX_BIGQUERY_DATASET_ID:-}}" \
      //tests"${1:-/...}" ${2+"$2"}
  fi
  set +x
}

function main() {
  if [[ $RUN_DATAPLEX = false && -n "${1:-}" ]]; then
    bazel_test ":$1"
  elif [[ "${1:-}" == "--run-dataplex" ]]; then
    bazel_test "/..." "--test_arg=--run-dataplex"
  elif [[ "${1:-}" == "test_dataplex"* ]]; then
    bazel_test ":$1" "--test_arg=--run-dataplex"
  else
    bazel_test
  fi
}

main "$@"