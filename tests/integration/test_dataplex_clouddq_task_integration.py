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

import pytest
from clouddq.integration.dq_dataplex import CloudDqDataplex
import json
import time
import datetime
import logging
import os


logger = logging.getLogger(__name__)

task_id = f"clouddq-test-task-{datetime.datetime.utcnow()}".replace(" ", "utc").replace(":", "-").replace(".", "-")

class TestDataplexIntegration:

    @pytest.fixture
    def gcp_project_id(self):
        gcp_project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', None)
        if not gcp_project_id:
            logger.fatal("Required test environment variable GOOGLE_CLOUD_PROJECT cannot be found. Set this to the project_id used for integration testing.")
        return gcp_project_id

    @pytest.fixture
    def gcp_bq_dataset(self):
        gcp_bq_dataset = os.environ.get('CLOUDDQ_BIGQUERY_DATASET', None)
        if not gcp_bq_dataset:
            logger.fatal("Required test environment variable CLOUDDQ_BIGQUERY_DATASET cannot be found. Set this to the BigQuery dataset used for integration testing.")
        return gcp_bq_dataset

    @pytest.fixture
    def gcp_bq_region(self):
        gcp_bq_region = os.environ.get('CLOUDDQ_BIGQUERY_REGION', None)
        if not gcp_bq_region:
            logger.fatal("Required test environment variable CLOUDDQ_BIGQUERY_REGION cannot be found. Set this to the BigQuery region used for integration testing.")
        return gcp_bq_region

    @pytest.fixture
    def gcp_sa_key(self):
        sa_key_path = os.environ.get('GOOGLE_SDK_CREDENTIALS', None)
        if not sa_key_path:
            logger.fatal("Required test environment variable GOOGLE_SDK_CREDENTIALS cannot be found. Set this to the exported service account key path used for integration testing.")
        return sa_key_path

    # @pytest.fixture
    # def gcp_impersonation_credentials(self):
    #     gcp_impersonation_credentials = os.environ.get('IMPERSONATION_SERVICE_ACCOUNT', None)
    #     if not gcp_impersonation_credentials:
    #         logger.fatal("Required test environment variable IMPERSONATION_SERVICE_ACCOUNT cannot be found. Set this to the service account name for impersonation used for integration testing.")
    #     return gcp_impersonation_credentials

    @pytest.fixture
    def gcp_bucket_name(self):
        gcp_bucket_name = os.environ.get('GCP_BUCKET_NAME', None)
        if not gcp_bucket_name:
            logger.fatal("Required test environment variable GCP_BUCKET_NAME cannot be found. Set this to the gcp bucket name having the clouddq zip file.")
        return gcp_bucket_name

    @pytest.fixture
    def test_dq_dataplex(self, gcp_project_id, gcp_bq_dataset, gcp_bq_region, gcp_bucket_name):

        dataplex_endpoint = "https://dataplex.googleapis.com"
        location_id = "us-central1"
        lake_name = "amandeep-dev-lake"
        # lake_name = "us-lake"
        gcp_project_id = gcp_project_id
        gcp_bucket_name = gcp_bucket_name
        gcp_bq_dataset = gcp_bq_dataset
        gcp_bq_region = gcp_bq_region

        return CloudDqDataplex(dataplex_endpoint, gcp_project_id,
                               location_id, lake_name, gcp_bucket_name,
                               gcp_bq_dataset, gcp_bq_region)

    def test_create_bq_dataplex_task_check_status_code_equals_200(self, test_dq_dataplex, gcp_bucket_name):
        """
        This test is for dataplex clouddq task api integration for bigquery
        :return:
        """
        print(f"Dataplex batches task id is {task_id}")
        print(test_dq_dataplex.gcp_bucket_name)
        print(f"GCP bucket name  {gcp_bucket_name}")
        trigger_spec_type: str = "ON_DEMAND"
        task_description: str = "clouddq task"
        data_quality_spec_file_path: str = f"gs://{gcp_bucket_name}/clouddq-configs.zip"
        result_dataset_name: str = "dataplex_clouddq"
        result_table_name: str = "target_table_dataplex"

        response = test_dq_dataplex.create_clouddq_task(
                    task_id,
                    trigger_spec_type,
                    task_description,
                    data_quality_spec_file_path,
                    result_dataset_name,
                    result_table_name)

        print(f"CloudDQ task creation response is {response.text}")
        assert response.status_code == 200

    def test_task_status_success(self, test_dq_dataplex):

        """
        This test is for getting the success status for CloudDQ Dataplex Task
        :return:
        """
        print(f"Dataplex batches task id is {task_id}")
        task_status = test_dq_dataplex.get_clouddq_task_status(task_id)
        print(f"CloudDQ task status is {task_status}")

        while (task_status != 'SUCCEEDED' and task_status != 'FAILED'):
            print(time.ctime())
            time.sleep(30)
            task_status = test_dq_dataplex.get_clouddq_task_status(task_id)
            print(task_status)

        assert task_status == 'SUCCEEDED'

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
