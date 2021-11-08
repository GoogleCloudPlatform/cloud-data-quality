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
source "$ROOT/scripts/common.sh"

# Check that all required env var are set
# if running locally you'd have to ensure the following are correctly set for your project/auth details
require_env_var GOOGLE_CLOUD_PROJECT "Set $GOOGLE_CLOUD_PROJECT to the project_id used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_DATASET "Set $CLOUDDQ_BIGQUERY_DATASET to the BigQuery dataset used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_REGION "Set $CLOUDDQ_BIGQUERY_REGION to the BigQuery region used for integration testing."
require_env_var GCS_BUCKET_NAME "Set $GCS_BUCKET_NAME to the GCS bucket name for staging CloudDQ artifacts and configs."
require_env_var DATAPLEX_LAKE_NAME "Set $DATAPLEX_LAKE_NAME to the Dataplex Lake used for testing."
require_env_var DATAPLEX_REGION_ID "Set DATAPLEX_REGION_ID to the region id of the Dataplex Lake. This should be the same as $CLOUDDQ_BIGQUERY_REGION."
require_env_var DATAPLEX_TARGET_BQ_DATASET "Set $DATAPLEX_TARGET_BQ_DATASET to the Target BQ Dataset used for testing. CloudDQ run fails if the dataset does not exist."
require_env_var DATAPLEX_TARGET_BQ_TABLE "Set $DATAPLEX_TARGET_BQ_TABLE to the Target BQ Table used for testing. The table will be created in $DATAPLEX_TARGET_BQ_DATASET if not already exist."
require_env_var DATAPLEX_TASK_SA "Set $DATAPLEX_TASK_SA to the service account used for running Dataplex Tasks in testing."

function bazel_test() {
  set -x
  bin/bazelisk test \
    --test_env GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}" \
    --test_env GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}" \
    --test_env GOOGLE_SDK_CREDENTIALS="${GOOGLE_SDK_CREDENTIALS:-}" \
    --test_env CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" \
    --test_env CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}" \
    --test_env IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT:-}" \
    --test_env GCS_BUCKET_NAME="${GCS_BUCKET_NAME}" \
    --test_env DATAPLEX_LAKE_NAME="${DATAPLEX_LAKE_NAME}" \
    --test_env DATAPLEX_REGION_ID="${DATAPLEX_REGION_ID}" \
    --test_env DATAPLEX_ENDPOINT="${DATAPLEX_ENDPOINT:-}" \
    --test_env DATAPLEX_TARGET_BQ_DATASET="${DATAPLEX_TARGET_BQ_DATASET}" \
    --test_env DATAPLEX_TARGET_BQ_TABLE="${DATAPLEX_TARGET_BQ_TABLE}" \
    --test_env DATAPLEX_TASK_SA="${DATAPLEX_TASK_SA}" \
    //tests"${1:-/...}"
  set +x
}

if [[ -n "${1:-}" ]]; then
  bazel_test ":$1"
else
  bazel_test
fi
