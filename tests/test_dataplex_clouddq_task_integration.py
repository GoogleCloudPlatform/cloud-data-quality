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

# Create and configure logger
logging.basicConfig(format='%(asctime)s %(message)s')

# Creating an object
logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

task_id = f"test-task-1-{datetime.datetime.utcnow()}".replace(" ", "utc").replace(":", "-").replace(".", "-")

class TestDataplexIntegration:

    @pytest.fixture
    def test_dq_dataplex(self):

        dataplex_endpoint = "https://dataplex.googleapis.com"
        location_id = 'us-central1'
        lake_name = "amandeep-dev-lake"
        project_id = "dataplex-clouddq"

        return CloudDqDataplex(dataplex_endpoint, project_id, location_id, lake_name)


    def test_create_bq_dataplex_task_check_status_code_equals_200(self, test_dq_dataplex):
        """
        This test is for dataplex clouddq task api integration for bigquery
        :return:
        """
        body = {
            "spark": {
                "python_script": "gs://dataplex-clouddq-api-integration-test/clouddq_pyspark_driver.py",
                "archive_uris": ["gs://dataplex-clouddq-api-integration-test/clouddq-configs.zip"],
                "file_uris": ["gs://dataplex-clouddq-api-integration-test/clouddq_patched.zip",
                              "gs://dataplex-clouddq-api-integration-test/clouddq_patched.zip.hashsum",
                              "gs://dataplex-clouddq-api-integration-test/profiles.yml"]
            },
            "execution_spec": {
                "args": {
                    "TASK_ARGS": "clouddq_patched.zip, ALL, configs, --dbt_profiles_dir=., --environment_target=bq, --skip_sql_validation"
                }
            },

            "trigger_spec": {
                "type": "ON_DEMAND"
            },
            "description": "task_1"
        }

        response = test_dq_dataplex.createDataplexTask(task_id, body)
        logger.info(response.text)
        assert response.status_code == 200


    def test_task_status_success(self, test_dq_dataplex):

        """
        This test is for getting the success status for CloudDQ Dataplex Task
        :return:
        """
        task_status = test_dq_dataplex.getDataplexTaskStatus(task_id)
        logger.info("Task Status %s", task_status)

        while (task_status != 'SUCCEEDED' and task_status != 'FAILED'):
            logger.info(time.ctime())
            time.sleep(30)
            task_status = test_dq_dataplex.getDataplexTaskStatus(task_id)
            logger.info(task_status)

        assert task_status == 'SUCCEEDED'


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
