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

set -x

PROJECT_ID="${PROJECT_ID}"
REGION="${REGION}"
ZONE="${ZONE}"
DATAPROC_CLUSTER_NAME="${DATAPROC_CLUSTER_NAME}"
GCS_BUCKET_NAME="${GCS_BUCKET_NAME}"
DQ_TARGET="${1:-ALL}"
ENVIRONMENT_TARGET="${2:-dev}"

function zip_configs_directory_and_upload_to_gcs() {
  zip -r clouddq-configs.zip configs
  gsutil cp clouddq-configs.zip gs://"${GCS_BUCKET_NAME}"/clouddq-configs.zip
}

function main() {
  zip_configs_directory_and_upload_to_gcs
  gcloud dataproc jobs submit pyspark \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --cluster "${DATAPROC_CLUSTER_NAME}" \
    --py-files=gs://clouddq-maxis/clouddq_executable.zip,gs://clouddq-maxis/clouddq_executable.zip.hashsum,profiles.yml \
    --archives=gs://"${GCS_BUCKET_NAME}"/clouddq-configs.zip \
    clouddq_pyspark_driver.py \
    -- \
    clouddq_executable.zip \
    "${DQ_TARGET}" \
    configs \
    --dbt_profiles_dir=. \
    --environment_target="${ENVIRONMENT_TARGET}"
}

main "$@"
