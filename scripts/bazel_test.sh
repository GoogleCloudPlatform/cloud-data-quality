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
require_env_var GOOGLE_CLOUD_PROJECT "Set this to the project_id used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_DATASET "Set this to the BigQuery dataset used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_REGION "Set this to the BigQuery region used for integration testing."
require_env_var GOOGLE_SDK_CREDENTIALS "Set this to the fully-qualified exported service account key path used for integration testing."
require_env_var IMPERSONATION_SERVICE_ACCOUNT "Set this to the service account name for impersonation used for integration testing."
require_env_var GCP_BUCKET_NAME "Set this to the GCP bucket name used for integration testing."

function bazel_test() {
  set -x
  bin/bazelisk test \
    --test_env GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}" \
    --test_env GOOGLE_SDK_CREDENTIALS="${GOOGLE_SDK_CREDENTIALS}" \
    --test_env CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" \
    --test_env CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}" \
    --test_env IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT}" \
    --test_env GCP_BUCKET_NAME="${GCP_BUCKET_NAME}" \
    //tests"${1:-/...}"
  set +x
}

if [[ -n "${1:-}" ]]; then
  bazel_test ":$1"
else
  bazel_test
fi
