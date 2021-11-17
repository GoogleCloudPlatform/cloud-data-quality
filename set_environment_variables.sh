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

# This file can be used to set the environments required to run `make test` or `scripts/bazel_test.sh`
# For testing, ensure you have created all the resources outlined in the environment variables below and set the environment variables to the correct resource IDs.
# You can then use this file to run tests with `source set_environment_variables.sh && make test`
# You may want to additionally ask git to ignore changes to `set_environment_variables.sh` to avoid accidentally committing secrets into git by running `git update-index --assume-unchanged set_environment_variables.sh`


# Set $GOOGLE_CLOUD_PROJECT to the project_id used for integration testing.
export GOOGLE_CLOUD_PROJECT="<your-project-id>"

# Set $CLOUDDQ_BIGQUERY_DATASET to the BigQuery dataset used for integration testing.
export CLOUDDQ_BIGQUERY_DATASET="<your-bigquery-dataset-id>"

# Set $CLOUDDQ_BIGQUERY_REGION to the BigQuery region used for integration testing.
export CLOUDDQ_BIGQUERY_REGION="<gcp-region-for-bigquery-jobs>"

# Set $GOOGLE_SDK_CREDENTIALS to the exported service account key path used for integration testing.
# If you have the environment variable GOOGLE_APPLICATION_CREDENTIALS set, you can do `export GOOGLE_SDK_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS}"`.
# If this environment variable is unset. All tests involving exported service account keys will be skipped.
# export GOOGLE_SDK_CREDENTIALS="<path-to-exported-service-account-key>"

# Set $IMPERSONATION_SERVICE_ACCOUNT to the service account name for impersonation used for integration testing
# If this environment variable is unset. All tests involving service account impersonation will be skipped.
# export IMPERSONATION_SERVICE_ACCOUNT="<service-account-name-for-impersonation>"

# Set $GCS_BUCKET_NAME to the GCS bucket name for staging CloudDQ artifacts and configs.
export GCS_BUCKET_NAME="<gcs-bucket-for-staging-clouddq-artifacts-and-configs>"

# Set $GCS_BAZEL_CACHE to the GCS bucket name for caching bazel builds.
export GCS_BAZEL_CACHE="<gcs-bucket-for-bazel-cache>"

# Set $GCS_CLOUDDQ_EXECUTABLE_PATH to the GCS bucket path for the pre-built CloudDQ files
# `clouddq-executable.zip` and `clouddq-executable.zip.hashsum`.
# If this is not set or empty, the test harness will look the the zip executable called
# `clouddq_patched.zip` on local path and upload it to $GCS_BUCKET_NAME for testing.
# export GCS_CLOUDDQ_EXECUTABLE_PATH="gs://dataplex-clouddq-artifacts-us-central1/"

# Set $DATAPLEX_LAKE_NAME to the Dataplex Lake used for testing.
export DATAPLEX_LAKE_NAME="<dataplex-lake-used-for-testing>"

# Set DATAPLEX_REGION_ID to the region id of the Dataplex Lake. This should be the same as $CLOUDDQ_BIGQUERY_REGION.
export DATAPLEX_REGION_ID="${CLOUDDQ_BIGQUERY_REGION}"

# Set $DATAPLEX_ENDPOINT to the Dataplex Endpoint used for testing.
export DATAPLEX_ENDPOINT="https://dataplex.googleapis.com"

# Set $DATAPLEX_TARGET_BQ_DATASET to the Target BQ Dataset used for testing. CloudDQ run fails if the dataset does not exist.
export DATAPLEX_TARGET_BQ_DATASET="<different-bq-dataset-for-storing-summary-results>"

# Set $DATAPLEX_TARGET_BQ_TABLE to the Target BQ Table used for testing. The table will be created in $DATAPLEX_TARGET_BQ_DATASET if not already exist.
export DATAPLEX_TARGET_BQ_TABLE="<table-name-in-$DATAPLEX_TARGET_BQ_DATASET>"

# Set $DATAPLEX_TASK_SA to the service account used for running Dataplex Tasks in testing.
export DATAPLEX_TASK_SA="<service-account-used-for-running-dataplex-task>"

# Set $DATAPLEX_ZONE_ID to the zone id in Dataplex Lake used for testing.
export DATAPLEX_ZONE_ID="<zone-id-in-$DATAPLEX_LAKE_NAME>"

# Set $DATAPLEX_BUCKET_NAME to the bucket name for GCS assets in Dataplex Lake used for testing.
export DATAPLEX_BUCKET_NAME="<zone-id-in-$DATAPLEX_BUCKET_NAME>"

# Set $DATAPLEX_BIGQUERY_DATASET_ID to the bigquery assets dataset id in Dataplex Lake used for testing.
export DATAPLEX_BIGQUERY_DATASET_ID="<zone-id-in-$DATAPLEX_BIGQUERY_DATASET_ID>"