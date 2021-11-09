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
import os
import pytest
import logging
import tempfile
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

@pytest.fixture
def gcp_project_id():
    gcp_project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', None)
    if not gcp_project_id:
        logger.fatal("Required test environment variable GOOGLE_CLOUD_PROJECT cannot be found. Set this to the project_id used for integration testing.")
    return gcp_project_id

@pytest.fixture
def gcp_bq_dataset():
    gcp_bq_dataset = os.environ.get('CLOUDDQ_BIGQUERY_DATASET', None)
    if not gcp_bq_dataset:
        logger.fatal("Required test environment variable CLOUDDQ_BIGQUERY_DATASET cannot be found. Set this to the BigQuery dataset used for integration testing.")
    return gcp_bq_dataset

@pytest.fixture
def gcp_bq_region():
    gcp_bq_region = os.environ.get('CLOUDDQ_BIGQUERY_REGION', None)
    if not gcp_bq_region:
        logger.fatal("Required test environment variable CLOUDDQ_BIGQUERY_REGION cannot be found. Set this to the BigQuery region used for integration testing.")
    return gcp_bq_region

@pytest.fixture
def gcp_application_credentials():
    gcp_application_credentials = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', None)
    if not gcp_application_credentials:
        logger.warning("Test environment variable GOOGLE_APPLICATION_CREDENTIALS cannot be found. Set this to the exported service account key path used for integration testing. The tests will proceed skipping all tests involving exported service-account key credentials.")
        if os.environ["GOOGLE_APPLICATION_CREDENTIALS"]:
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    return gcp_application_credentials

@pytest.fixture
def gcp_sa_key():
    sa_key_path = os.environ.get('GOOGLE_SDK_CREDENTIALS', None)
    if not sa_key_path:
        logger.warning("Test environment variable GOOGLE_SDK_CREDENTIALS cannot be found. Set this to the exported service account key path used for integration testing. The tests will proceed skipping all tests involving exported service-account key credentials.")
        if os.environ["GOOGLE_SDK_CREDENTIALS"]:
            del os.environ["GOOGLE_SDK_CREDENTIALS"]
    return sa_key_path

@pytest.fixture
def gcp_impersonation_credentials():
    gcp_impersonation_credentials = os.environ.get('IMPERSONATION_SERVICE_ACCOUNT', None)
    if not gcp_impersonation_credentials:
        logger.warning("Test environment variable IMPERSONATION_SERVICE_ACCOUNT cannot be found. Set this to the service account name for impersonation used for integration testing. The tests will proceed skipping all tests involving service-account impersonation.")
        if os.environ["IMPERSONATION_SERVICE_ACCOUNT"]:
            del os.environ["IMPERSONATION_SERVICE_ACCOUNT"]
    return gcp_impersonation_credentials

@pytest.fixture
def gcs_bucket_name():
    gcs_bucket_name = os.environ.get('GCS_BUCKET_NAME', None)
    if not gcs_bucket_name:
        logger.fatal("Required test environment variable GCS_BUCKET_NAME cannot be found. Set this to the GCS bucket name for staging CloudDQ artifacts and configs.")
    return gcs_bucket_name

@pytest.fixture
def gcs_clouddq_executable_path():
    gcs_clouddq_executable_path = os.environ.get('GCS_CLOUDDQ_EXECUTABLE_PATH', None)
    if not gcs_clouddq_executable_path:
        logger.warning(
            "Test environment variable GCS_CLOUDDQ_EXECUTABLE_PATH cannot be found. "
            "Set this to the GCS bucket path for the pre-built CloudDQ file `clouddq-executable.zip` "
            " and `clouddq-executable.zip.hashsum`."
            "If this is not set or empty, the test harness will look the the zip executable at "
            f"`bazel-bin/clouddq/clouddq_patched.zip` and upload it to $GCS_BUCKET_NAME for testing.")
    return gcs_clouddq_executable_path

@pytest.fixture
def gcp_dataplex_region():
    gcp_dataplex_region = os.environ.get('DATAPLEX_REGION_ID', None)
    if not gcp_dataplex_region:
        logger.fatal("Required test environment variable DATAPLEX_REGION_ID cannot be found. Set this to the region id of the Dataplex Lake.")
    return gcp_dataplex_region

@pytest.fixture
def gcp_dataplex_lake_name():
    gcp_dataplex_lake_name = os.environ.get('DATAPLEX_LAKE_NAME', None)
    if not gcp_dataplex_lake_name:
        logger.fatal("Required test environment variable DATAPLEX_LAKE_NAME cannot be found. Set this to the Dataplex Lake used for testing.")
    return gcp_dataplex_lake_name

@pytest.fixture
def dataplex_endpoint():
    dataplex_endpoint = os.environ.get('DATAPLEX_ENDPOINT', None)
    if not dataplex_endpoint:
        logger.warning("Required test environment variable DATAPLEX_ENDPOINT cannot be found. Defaulting to the Dataplex Endpoint 'https://dataplex.googleapis.com'.")
        dataplex_endpoint = "https://dataplex.googleapis.com"
    return dataplex_endpoint

@pytest.fixture
def target_bq_result_dataset_name():
    target_bq_result_dataset_name = os.environ.get('DATAPLEX_TARGET_BQ_DATASET', None)
    if not target_bq_result_dataset_name:
        logger.fatal("Required test environment variable DATAPLEX_TARGET_BQ_DATASET cannot be found. Set this to the Target BQ Dataset used for testing.")
    return target_bq_result_dataset_name

@pytest.fixture
def target_bq_result_table_name():
    target_bq_result_table_name = os.environ.get('DATAPLEX_TARGET_BQ_TABLE', None)
    if not target_bq_result_table_name:
        logger.fatal("Required test environment variable DATAPLEX_TARGET_BQ_TABLE cannot be found. Set this to the Target BQ Table used for testing.")
    return target_bq_result_table_name

@pytest.fixture
def dataplex_task_service_account_name():
    dataplex_task_service_account_name = os.environ.get('DATAPLEX_TASK_SA', None)
    if not dataplex_task_service_account_name:
        logger.fatal("Required test environment variable DATAPLEX_TASK_SA cannot be found. Set this to the service account used for running Dataplex Tasks in testing.")
    return dataplex_task_service_account_name

@pytest.fixture
def temp_clouddq_dir(gcp_project_id, gcp_bq_dataset):
    # Create temp directory
    source_configs_path = Path("tests").joinpath("resources", "configs")
    temp_clouddq_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test_artifacts")
    # Clean directory if exists
    if os.path.exists(temp_clouddq_dir):
        shutil.rmtree(temp_clouddq_dir)
    # Copy over tests/resources/configs
    configs_path = Path(temp_clouddq_dir).joinpath("configs")
    _ = shutil.copytree(source_configs_path, configs_path)
    # Prepare test config
    test_data = configs_path.joinpath("entities", "test-data.yml")
    with open(test_data) as source_file:
        lines = source_file.read()
    with open(test_data, "w") as source_file:
        lines = lines.replace("<your_gcp_project_id>", gcp_project_id)
        lines = lines.replace("dq_test", gcp_bq_dataset)
        source_file.write(lines)
    yield temp_clouddq_dir
    shutil.rmtree(temp_clouddq_dir)

def pytest_configure(config):
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")
