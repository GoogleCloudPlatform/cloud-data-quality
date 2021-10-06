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

# get diagnostic info
which python3
python3 --version
python3 -m pip --version

# create temporary virtualenv
python3 -m venv /tmp/clouddq_test_env
source /tmp/clouddq_test_env/bin/activate

# install clouddq wheel into temporary env
python3 -m pip install .

# set variables
# note this only works in github actions
# if running locally you'd have to ensure the following are correctly set for your project/auth details
GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project)" || err "Test cannot proceed unless environment variable GOOGLE_CLOUD_PROJECT is set to the test project_id"
CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" || err "Test cannot proceed unless environment variable CLOUDDQ_BIGQUERY_DATASET is set to the test gcp_bq_dataset."
CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}" || err "Test cannot proceed unless environment variable CLOUDDQ_BIGQUERY_REGION is set to the test gcp_bq_region."
GOOGLE_APPLICATION_CREDENTIAL="${GOOGLE_APPLICATION_CREDENTIALS}" || err "Test cannot proceed unless environment variable GOOGLE_APPLICATION_CREDENTIALS is set to the test service_account key path."
IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT}" || err "Test cannot proceed unless environment variable GOOGLE_IMPERSONATION_CREDENTIALS is set to the service_account impersonation ID."

# test clouddq help
python3 clouddq --help

# test clouddq in isolated directory with minimal file dependencies
TEST_DIR=/tmp/clouddq-test-pip
rm -rf "$TEST_DIR"
mkdir "$TEST_DIR"
cp -r configs "$TEST_DIR"
cp tests/resources/test_dbt_profiles_dir/profiles.yml "$TEST_DIR"
cd "$TEST_DIR"
sed -i s/\<your_gcp_project_id\>/"${GOOGLE_CLOUD_PROJECT}"/g "$TEST_DIR"/profiles.yml
sed -i s/clouddq/"${CLOUDDQ_BIGQUERY_DATASET}"/g "$TEST_DIR"/profiles.yml
sed -i s/EU/"${CLOUDDQ_BIGQUERY_REGION}"/g "$TEST_DIR"/profiles.yml
python3 -m clouddq ALL configs --dbt_profiles_dir="$TEST_DIR" --debug --dry_run
python3 -m clouddq ALL configs --dbt_profiles_dir="$TEST_DIR" --dbt_path="$TEST_DIR" --debug --dry_run

# set-up service account application-default credentials
gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}"

# test clouddq with direct connection profiles
python3 -m clouddq ALL configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
    --debug \
    --dry_run \
    --skip_sql_validation

# test clouddq with exported service account key
python3 -m clouddq ALL configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
    --gcp_service_account_key_path="${GOOGLE_APPLICATION_CREDENTIALS}" \
    --debug \
    --dry_run \
    --skip_sql_validation

# test clouddq with exported service account key
python3 -m clouddq ALL configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
    --gcp_service_account_key_path="${GOOGLE_APPLICATION_CREDENTIALS}" \
    --gcp_impersonation_credentials="${IMPERSONATION_SERVICE_ACCOUNT}" \
    --debug \
    --dry_run \
    --skip_sql_validation

# test clouddq with service account impersonation
python3 -m clouddq ALL configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
    --gcp_impersonation_credentials="${IMPERSONATION_SERVICE_ACCOUNT}" \
    --debug \
    --dry_run \
    --skip_sql_validation