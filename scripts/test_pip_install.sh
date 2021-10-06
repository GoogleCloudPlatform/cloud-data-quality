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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/common.sh"

# Check that all required env var are set
require_env_var GOOGLE_CLOUD_PROJECT
require_env_var CLOUDDQ_BIGQUERY_DATASET
require_env_var CLOUDDQ_BIGQUERY_REGION
require_env_var GOOGLE_APPLICATION_CREDENTIAL
require_env_var IMPERSONATION_SERVICE_ACCOUNT

# set variables
# if running locally you'd have to ensure the following are correctly set for your project/auth details
GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project)"
CLOUDDQ_BIGQUERY_DATASET="${CLOUDDQ_BIGQUERY_DATASET}" 
CLOUDDQ_BIGQUERY_REGION="${CLOUDDQ_BIGQUERY_REGION}"
GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS}"
IMPERSONATION_SERVICE_ACCOUNT="${IMPERSONATION_SERVICE_ACCOUNT}"

# get diagnostic info
which python3
python3 --version
python3 -m pip --version

# create temporary virtualenv
python3 -m venv /tmp/clouddq_test_env
source /tmp/clouddq_test_env/bin/activate

# install clouddq wheel into temporary env
python3 -m pip install .

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