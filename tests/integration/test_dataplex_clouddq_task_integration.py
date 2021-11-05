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
from clouddq.integration.dataplex.clouddq_dataplex import CloudDqDataplex
from clouddq.integration.dataplex import clouddq_pyspark_driver
from clouddq.integration.gcs import upload_blob
from clouddq.utils import working_directory
from pprint import pformat
import time
import datetime
import logging
import shutil
from pathlib import Path


logger = logging.getLogger(__name__)

class TestDataplexIntegration:

    @pytest.fixture
    def temp_artifacts_path(self, temp_clouddq_dir):
        with working_directory(temp_clouddq_dir):
            # Create standard configs zip called 'clouddq-configs.zip'
            configs_path = Path(temp_clouddq_dir).joinpath("configs")
            shutil.make_archive('clouddq-configs', 'zip', configs_path.parent, configs_path.name)
            # Create non-standard configs zip without top-level configs directory
            #  called 'non-standard-clouddq-configs.zip'
            shutil.make_archive('non-standard-clouddq-configs', 'zip', configs_path)
            # Create empty configs zip called 'empty-clouddq-configs.zip'
            empty_directory = Path(temp_clouddq_dir).joinpath("empty")
            empty_directory.mkdir()
            shutil.make_archive('empty-clouddq-configs', 'zip', empty_directory.parent, empty_directory.name)
            # Create single YAML config called 'configs.yml'
            configs_content = []
            for file in configs_path.glob("**/*.yml"):
                configs_content.append(file.open().read())
            for file in configs_path.glob("**/*.yaml"):
                configs_content.append(file.open().read())
            single_yaml_path = Path(temp_clouddq_dir).joinpath("configs.yml")
            single_yaml_path.write_text("\n".join(configs_content))
            # Create single malformed YAML config called 'malformed-configs.yml'
            single_malformed_yaml_path = Path(temp_clouddq_dir).joinpath("malformed-configs.yml")
            single_malformed_yaml_path.write_text("\n".join([c for c in configs_content if "entities:" not in c]))
            # Print temp configs path
            print(pformat(list(temp_clouddq_dir.glob("**/*"))))
            # Return path and delete when done
            yield temp_clouddq_dir

    @pytest.fixture
    def gcs_clouddq_configs_standard(self, temp_artifacts_path, gcs_bucket_name):
        file_name = "clouddq-configs.zip"
        configs_file_path = temp_artifacts_path.joinpath(file_name)
        upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def gcs_clouddq_configs_nonstandard(self, temp_artifacts_path, gcs_bucket_name):
        file_name = "non-standard-clouddq-configs.zip"
        configs_file_path = temp_artifacts_path.joinpath(file_name)
        upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def gcs_clouddq_configs_empty(self, temp_artifacts_path, gcs_bucket_name):
        file_name = "empty-clouddq-configs.zip"
        configs_file_path = temp_artifacts_path.joinpath(file_name)
        upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def gcs_clouddq_configs_single_yaml(self, temp_artifacts_path, gcs_bucket_name):
        file_name = "configs.yml"
        configs_file_path = temp_artifacts_path.joinpath(file_name)
        upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def gcs_clouddq_configs_single_yaml_malformed(self, temp_artifacts_path, gcs_bucket_name):
        file_name = "malformed-configs.yml"
        configs_file_path = temp_artifacts_path.joinpath(file_name)
        upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def gcs_clouddq_pyspark_driver(self, gcs_bucket_name):
        file_name = 'clouddq_pyspark_driver.py'
        driver_path = clouddq_pyspark_driver.__file__
        upload_blob(gcs_bucket_name, driver_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def test_dq_dataplex_client(self,
                                dataplex_endpoint,
                                gcp_dataplex_lake_name,
                                gcp_dataplex_region,
                                gcp_project_id,
                                gcp_bq_dataset,
                                gcp_bq_region,
                                gcs_bucket_name):
        gcp_project_id = gcp_project_id
        gcp_bq_dataset = gcp_bq_dataset
        gcp_bq_region = gcp_bq_region
        gcs_bucket_name = gcs_bucket_name
        yield CloudDqDataplex(dataplex_endpoint=dataplex_endpoint,
                               gcp_dataplex_lake_name=gcp_dataplex_lake_name,
                               gcp_dataplex_region=gcp_dataplex_region,
                               gcp_project_id=gcp_project_id,
                               gcs_bucket_name=gcs_bucket_name,
                               gcp_bq_dataset=gcp_bq_dataset,
                               gcp_bq_region=gcp_bq_region,
                               gcp_service_account_key_path=None,
                               gcp_impersonation_credentials=None)

    @pytest.mark.parametrize(
        "input_configs,expected",
        [
            pytest.param(
                'gcs_clouddq_configs_standard',
                "SUCCEEDED",
                id="configs_standard"
            ),
            pytest.param(
                'gcs_clouddq_configs_nonstandard',
                "SUCCEEDED",
                id="configs_nonstandard"
            ),
            pytest.param(
                'gcs_clouddq_configs_empty',
                "FAILED",
                id="configs_empty"
            ),
            pytest.param(
                'gcs_clouddq_configs_single_yaml',
                "SUCCEEDED",
                id="configs_single_yaml"
            ),
            pytest.param(
                'gcs_clouddq_configs_single_yaml_malformed',
                "FAILED",
                id="configs_single_yaml_malformed"
            ),
        ],
    )
    def test_create_bq_dataplex_task(self,
                    test_dq_dataplex_client,
                    input_configs,
                    expected,
                    gcs_clouddq_pyspark_driver,
                    target_bq_result_dataset_name,
                    target_bq_result_table_name,
                    dataplex_task_service_account_name,
                    request):
        """
        This test is for dataplex clouddq task api integration for bigquery
        :return:
        """
        test_id = f"{request.node.callspec.id}".replace("_", "-")
        task_id = f"clouddq-test-create-dataplex-task-{test_id}"
        print(f"Dataplex batches task id is: {task_id}")
        print(f"Delete task_id if it already exists...")
        response = test_dq_dataplex_client.delete_clouddq_task_if_exists(task_id)
        print(f"CloudDQ task deletion response is {response.text}")
        assert response.status_code == 200 or response.status_code == 404
        data_quality_spec_file_path: str = request.getfixturevalue(input_configs)
        print(f"Using test GCP configs: {data_quality_spec_file_path}")
        trigger_spec_type: str = "ON_DEMAND"
        task_description: str = f"clouddq task created at {datetime.datetime.utcnow()} for test {test_id}"
        clouddq_pyspark_driver_path: str = gcs_clouddq_pyspark_driver
        labels: dict = {"task_type": "test"}
        response = test_dq_dataplex_client.create_clouddq_task(
                    task_id=task_id,
                    trigger_spec_type=trigger_spec_type,
                    task_description=task_description,
                    data_quality_spec_file_path=data_quality_spec_file_path,
                    result_dataset_name=target_bq_result_dataset_name,
                    result_table_name=target_bq_result_table_name,
                    service_account=dataplex_task_service_account_name,
                    labels=labels,
                    clouddq_pyspark_driver_path=clouddq_pyspark_driver_path)
        print(f"CloudDQ task creation response is {response.text}")
        assert response.status_code == 200
        task_status = test_dq_dataplex_client.get_clouddq_task_status(task_id)
        while (task_status != 'SUCCEEDED' and task_status != 'FAILED'):
            print(time.ctime())
            time.sleep(30)
            task_status = test_dq_dataplex_client.get_clouddq_task_status(task_id)
            print(f"CloudDQ task status is {task_status}")
        assert task_status == expected

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))