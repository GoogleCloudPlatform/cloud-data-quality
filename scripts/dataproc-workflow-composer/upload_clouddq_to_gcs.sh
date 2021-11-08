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

GCS_BUCKET_NAME="${GCS_BUCKET_NAME}" || err "Environment variable GCS_BUCKET_NAME is not set." 
CLOUDDQ_RELEASE_VERSION="${CLOUDDQ_RELEASE_VERSION}" || err "Environment variable CLOUDDQ_RELEASE_VERSION is not set." 

function zip_configs_directory_and_upload_to_gcs() {
  zip -r clouddq-configs.zip ./configs
  gsutil mv clouddq-configs.zip gs://"${GCS_BUCKET_NAME}"/clouddq-configs.zip
  gsutil ls gs://"${GCS_BUCKET_NAME}"/clouddq_pyspark_driver.py || gsutil cp ./clouddq/integration/clouddq_pyspark_driver.py gs://"${GCS_BUCKET_NAME}"
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
}

main "$@"