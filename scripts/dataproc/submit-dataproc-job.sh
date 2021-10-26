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

set -x

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

PROJECT_ID="${PROJECT_ID}" || err "Environment variable PROJECT_ID is not set." 
REGION="${REGION}" || err "Environment variable REGION is not set." 
ZONE="${ZONE}" || err "Environment variable ZONE is not set." 
DATAPROC_CLUSTER_NAME="${DATAPROC_CLUSTER_NAME}" || err "Environment variable DATAPROC_CLUSTER_NAME is not set." 
GCS_BUCKET_NAME="${GCS_BUCKET_NAME}" || err "Environment variable GCS_BUCKET_NAME is not set." 
CLOUDDQ_RELEASE_VERSION="${CLOUDDQ_RELEASE_VERSION}" || err "Environment variable CLOUDDQ_RELEASE_VERSION is not set." 
DQ_TARGET="${1:-ALL}"
ENVIRONMENT_TARGET="${2:-dev}"

function zip_configs_directory_and_upload_to_gcs() {
  zip -r clouddq-configs.zip configs
  gsutil cp clouddq-configs.zip gs://"${GCS_BUCKET_NAME}"/clouddq-configs.zip
}

function upload_clouddq_zip_executable_to_gcs() {
  wget -O /tmp/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v"${CLOUDDQ_RELEASE_VERSION}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}"_linux-amd64.zip
  wget -O /tmp/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip.hashsum https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v"${CLOUDDQ_RELEASE_VERSION}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}"_linux-amd64.zip.sha256sum
  gsutil cp /tmp/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip gs://"${GCS_BUCKET_NAME}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip
  gsutil cp /tmp/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip.hashsum gs://"${GCS_BUCKET_NAME}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip.hashsum
}

function main() {
  zip_configs_directory_and_upload_to_gcs
  gsutil ls gs://"${GCS_BUCKET_NAME}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip gs://"${GCS_BUCKET_NAME}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip.hashsum || upload_clouddq_zip_executable_to_gcs
  gcloud dataproc jobs submit pyspark \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --cluster "${DATAPROC_CLUSTER_NAME}" \
    --py-files=gs://"${GCS_BUCKET_NAME}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip,gs://"${GCS_BUCKET_NAME}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip.hashsum,dbt/profiles.yml \
    --archives=gs://"${GCS_BUCKET_NAME}"/clouddq-configs.zip \
    scripts/dataproc/clouddq_pyspark_driver.py \
    -- \
    clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}".zip \
    "${DQ_TARGET}" \
    configs \
    --dbt_profiles_dir=. \
    --environment_target="${ENVIRONMENT_TARGET}"
}

main "$@"
