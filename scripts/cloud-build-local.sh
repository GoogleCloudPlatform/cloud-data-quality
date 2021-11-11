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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/common.sh"

# Check that all required env var are set
require_binary cloud-build-local "Install cloud-build-local before continuing."

# Check that all required env var are set
require_env_var GOOGLE_CLOUD_PROJECT "Set this to the project_id used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_DATASET "Set this to the BigQuery dataset used for integration testing."
require_env_var CLOUDDQ_BIGQUERY_REGION "Set this to the BigQuery region used for integration testing."
require_env_var GCS_BUCKET_NAME
require_env_var DATAPLEX_LAKE_NAME
require_env_var DATAPLEX_REGION_ID
require_env_var DATAPLEX_ENDPOINT
require_env_var DATAPLEX_TARGET_BQ_DATASET
require_env_var DATAPLEX_TARGET_BQ_TABLE
require_env_var DATAPLEX_TASK_SA

# set variables
# if running locally you'd have to ensure the following are correctly set for your project/auth details
GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}"
CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" 
CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}"
GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-}"
IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT:-}"
GCS_BUCKET_NAME="${GCS_BUCKET_NAME}"
DATAPLEX_LAKE_NAME="${DATAPLEX_LAKE_NAME}"
DATAPLEX_REGION_ID="${DATAPLEX_REGION_ID}"
DATAPLEX_ENDPOINT="${DATAPLEX_ENDPOINT}"
DATAPLEX_TARGET_BQ_DATASET="${DATAPLEX_TARGET_BQ_DATASET}"
DATAPLEX_TARGET_BQ_TABLE="${DATAPLEX_TARGET_BQ_TABLE}"
DATAPLEX_TASK_SA="${DATAPLEX_TASK_SA}"

DRY_RUN=false
if [[ $# -eq 0 ]]; then
  echo "DRY_RUN: ${DRY_RUN}"
else
  if [[ "$1" == "--dry-run" ]]; then
      DRY_RUN=true
      echo "DRY_RUN: $DRY_RUN"
  elif [[ "$1" == "--dry-run=false" ]]; then
      DRY_RUN=false
      echo "DRY_RUN: $DRY_RUN"
  fi
fi

cloud-build-local \
--config=cloudbuild.yaml \
--write-workspace=cloudbuild-workspace \
--dryrun=${DRY_RUN} \
--substitutions \
_GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}",\
_CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}",\
_CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}",\
_GCS_BUCKET_NAME="${GCS_BUCKET_NAME}",\
_DATAPLEX_LAKE_NAME="${DATAPLEX_LAKE_NAME}",\
_DATAPLEX_REGION_ID="${DATAPLEX_REGION_ID}",\
_DATAPLEX_ENDPOINT="${DATAPLEX_ENDPOINT}",\
_DATAPLEX_TARGET_BQ_DATASET="${DATAPLEX_TARGET_BQ_DATASET}",\
_DATAPLEX_TARGET_BQ_TABLE="${DATAPLEX_TARGET_BQ_TABLE}",\
_DATAPLEX_TASK_SA="${DATAPLEX_TASK_SA}" \
.