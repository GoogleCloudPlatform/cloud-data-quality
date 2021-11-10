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
import hashlib
from clouddq.integration.dataplex.clouddq_dataplex import CloudDqDataplexClient
from clouddq.integration import clouddq_pyspark_driver
from clouddq.integration import test_clouddq_pyspark_driver
from clouddq.integration.gcs import upload_blob
from clouddq.utils import working_directory
from pprint import pformat
import time
import datetime
import json
import logging
import shutil
from pathlib import Path
from pprint import pprint

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
    def gcs_clouddq_configs_nonstandard_local(self, temp_artifacts_path):
        file_name = "non-standard-clouddq-configs.zip"
        configs_file_path = temp_artifacts_path.joinpath(file_name)
        return configs_file_path.absolute()

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
    def test_gcs_clouddq_pyspark_driver(self, gcs_bucket_name):
        file_name = 'test_clouddq_pyspark_driver.py'
        driver_path = test_clouddq_pyspark_driver.__file__
        upload_blob(gcs_bucket_name, driver_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        return gcs_uri

    @pytest.fixture
    def test_clouddq_zip_executable_paths(self,
                                gcs_bucket_name,
                                gcs_clouddq_executable_path):
        if gcs_clouddq_executable_path:
            gcs_zip_executable_name = f"{gcs_clouddq_executable_path}/clouddq-executable.zip"
            gcs_zip_executable_hashsum_name = f"{gcs_clouddq_executable_path}/clouddq-executable.zip.hashsum"
        else:
            print(Path().absolute())
            pprint(list(Path().iterdir()))
            clouddq_zip_build = Path("clouddq_patched.zip")
            if not clouddq_zip_build.resolve().is_file():
                raise RuntimeError(
                    f"Local CloudDQ Artifact Zip at `{clouddq_zip_build.absolute()}` "
                    "not found. Run `make build` before continuing.")
            upload_blob(gcs_bucket_name, clouddq_zip_build, "test/clouddq-executable.zip")
            gcs_zip_executable_name = f"gs://{gcs_bucket_name}/test/clouddq-executable.zip"
            hash_sha256 = hashlib.sha256()
            with open(clouddq_zip_build, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            hashsum = hash_sha256.hexdigest()
            hashsum_file = Path("clouddq_patched.zip.hashsum")
            hashsum_file.write_text(hashsum)
            upload_blob(gcs_bucket_name, hashsum_file, "test/clouddq-executable.zip.hashsum")
            gcs_zip_executable_hashsum_name = f"gs://{gcs_bucket_name}/test/clouddq-executable.zip.hashsum"
        return (gcs_zip_executable_name, gcs_zip_executable_hashsum_name)

    @pytest.fixture
    def test_dq_dataplex_client(self,
                                dataplex_endpoint,
                                gcp_dataplex_lake_name,
                                gcp_dataplex_region,
                                gcp_project_id,
                                gcs_bucket_name):
        gcp_project_id = gcp_project_id
        gcs_bucket_name = gcs_bucket_name
        yield CloudDqDataplexClient(dataplex_endpoint=dataplex_endpoint,
                               gcp_dataplex_lake_name=gcp_dataplex_lake_name,
                               gcp_dataplex_region=gcp_dataplex_region,
                               gcp_project_id=gcp_project_id,
                               gcs_bucket_name=gcs_bucket_name)

    def test_get_dataplex_lake_success(self,
                                test_dq_dataplex_client,
                                gcp_project_id,
                                gcp_dataplex_region,
                                gcp_dataplex_lake_name):
        response = test_dq_dataplex_client.get_dataplex_lake(gcp_dataplex_lake_name)
        assert response.status_code == 200
        resp_obj = json.loads(response.text)
        expected_name = f"projects/{gcp_project_id}/locations/{gcp_dataplex_region}/lakes/{gcp_dataplex_lake_name}"
        assert resp_obj["name"] == expected_name

    @pytest.mark.parametrize(
        "input_configs,expected",
        [
            pytest.param(
                'gcs_clouddq_configs_standard',
                "SUCCEEDED",
                id="without_clouddq_executable"
            ),
        ]
    )
    def test_create_clouddq_task_no_executable(self,
                            test_dq_dataplex_client,
                            input_configs,
                            expected,
                            test_gcs_clouddq_pyspark_driver,
                            gcp_project_id,
                            gcp_bq_dataset,
                            gcp_bq_region,
                            target_bq_result_dataset_name,
                            target_bq_result_table_name,
                            dataplex_task_service_account_name,
                            request):

        # Prepare the test YAML configurations from fixture
        clouddq_yaml_spec_file_path: str = request.getfixturevalue(input_configs)
        # Use the test clouddq_pyspark_driver
        clouddq_pyspark_driver_path: str = test_gcs_clouddq_pyspark_driver
        # Prepare Task_ID for reference
        test_id = f"{request.node.callspec.id}".replace("_", "-")
        task_id = f"clouddq-test-create-dataplex-task-{test_id}"
        print(f"Dataplex batches task id is: {task_id}")
        #     # Clean-up old task_ids if exists
        print(f"Delete task_id if it already exists...")
        response = test_dq_dataplex_client.delete_clouddq_task_if_exists(task_id)
        print(f"CloudDQ task deletion response is {response.text}")
        # Continue if Task_ID is successfully deleted or not found
        assert response.status_code == 200 or response.status_code == 404
        # Set other variables not in scope for testing
        task_trigger_spec_type: str = "ON_DEMAND"
        task_description: str = f"clouddq task created at {datetime.datetime.utcnow()} for test {test_id}"
        task_labels: dict = {"test_id": test_id}
        # Assumes target bq result dataset exists in the same project as the test fixture `gcp_project_id`
        target_bq_result_project_name = gcp_project_id

        # Create Dataplex Task with test arguments
        response = test_dq_dataplex_client.create_clouddq_task(
                        task_id=task_id,
                        clouddq_yaml_spec_file_path=clouddq_yaml_spec_file_path,
                        clouddq_run_project_id=gcp_project_id,
                        clouddq_run_bq_region=gcp_bq_region,
                        clouddq_run_bq_dataset=gcp_bq_dataset,
                        target_bq_result_project_name=target_bq_result_project_name,
                        target_bq_result_dataset_name=target_bq_result_dataset_name,
                        target_bq_result_table_name=target_bq_result_table_name,
                        task_service_account=dataplex_task_service_account_name,
                        task_trigger_spec_type=task_trigger_spec_type,
                        task_description=task_description,
                        task_labels=task_labels,
                        clouddq_pyspark_driver_path=clouddq_pyspark_driver_path,
                        clouddq_pyspark_driver_filename="test_clouddq_pyspark_driver.py",
                        clouddq_executable_path=None,
                        clouddq_executable_checksum_path=None,)
        print(response.text)
        assert response.status_code == 200
        # Poll task status until it has either succeeded or failed
        # Tests will timeout if exceeding the duration `--test_timeout` set in `.bazelrc`
        task_status = test_dq_dataplex_client.get_clouddq_task_status(task_id)
        while (task_status != 'SUCCEEDED' and task_status != 'FAILED'):
            print(time.ctime())
            time.sleep(30)
            task_status = test_dq_dataplex_client.get_clouddq_task_status(task_id)
            print(f"CloudDQ task status is {task_status}")
        assert task_status == expected
    
    @pytest.mark.parametrize(
        "input_configs,validate_only,expected",
        [
            # pytest.param(
            #     'gcs_clouddq_configs_standard',
            #     True,
            #     "SUCCEEDED",
            #     id="validate_only"
            # ),
            pytest.param(
                'gcs_clouddq_configs_standard',
                False,
                "SUCCEEDED",
                id="configs_standard"
            ),
            # pytest.param(
            #     'gcs_clouddq_configs_nonstandard',
            #     False,
            #     "SUCCEEDED",
            #     id="configs_nonstandard"
            # ),
            # pytest.param(
            #     'gcs_clouddq_configs_nonstandard_local',
            #     False,
            #     "SUCCEEDED",
            #     id="configs_nonstandard_local"
            # ),
            # pytest.param(
            #     'gcs_clouddq_configs_empty',
            #     False,
            #     "FAILED",
            #     id="configs_empty"
            # ),
            # pytest.param(
            #     'gcs_clouddq_configs_single_yaml',
            #     False,
            #     "SUCCEEDED",
            #     id="configs_single_yaml"
            # ),
            # pytest.param(
            #     'gcs_clouddq_configs_single_yaml_malformed',
            #     False,
            #     "FAILED",
            #     id="configs_single_yaml_malformed"
            # ),
        ],
    )
    def test_create_bq_dataplex_task(self,
                    test_dq_dataplex_client,
                    input_configs,
                    test_clouddq_zip_executable_paths,
                    validate_only,
                    expected,
                    gcs_clouddq_pyspark_driver,
                    gcp_project_id,
                    gcp_bq_dataset,
                    gcp_bq_region,
                    target_bq_result_dataset_name,
                    target_bq_result_table_name,
                    dataplex_task_service_account_name,
                    request):
        """
        This test is for dataplex clouddq task api integration for bigquery
        :return:
        """
        # Get executable path
        print(f"Using executable paths: {test_clouddq_zip_executable_paths}")
        clouddq_executable_path, clouddq_executable_checksum_path = test_clouddq_zip_executable_paths
        # Prepare the test YAML configurations from fixture
        clouddq_yaml_spec_file_path: str = request.getfixturevalue(input_configs)
        print(f"Using test GCP configs: {clouddq_yaml_spec_file_path}")
        # Use the most updated clouddq_pyspark_driver file from the project for testing
        clouddq_pyspark_driver_path: str = gcs_clouddq_pyspark_driver
        # Prepare Task_ID for reference
        test_id = f"{request.node.callspec.id}".replace("_", "-")
        task_id = f"clouddq-test-create-dataplex-task-{test_id}"
        print(f"Dataplex batches task id is: {task_id}")
        # Clean-up old task_ids if exists
        print(f"Delete task_id if it already exists...")
        response = test_dq_dataplex_client.delete_clouddq_task_if_exists(task_id)
        print(f"CloudDQ task deletion response is {response.text}")
        # Continue if Task_ID is successfully deleted or not found
        assert response.status_code == 200 or response.status_code == 404
        # Set other variables not in scope for testing
        task_trigger_spec_type: str = "ON_DEMAND"
        task_description: str = f"clouddq task created at {datetime.datetime.utcnow()} for test {test_id}"
        task_labels: dict = {"test_id": test_id}
        # Assumes target bq result dataset exists in the same project as the test fixture `gcp_project_id`
        target_bq_result_project_name = gcp_project_id
        # Create Dataplex Task with test arguments
        response = test_dq_dataplex_client.create_clouddq_task(
                    task_id=task_id,
                    clouddq_yaml_spec_file_path=clouddq_yaml_spec_file_path,
                    clouddq_run_project_id=gcp_project_id,
                    clouddq_run_bq_region=gcp_bq_region,
                    clouddq_run_bq_dataset=gcp_bq_dataset,
                    target_bq_result_project_name=target_bq_result_project_name,
                    target_bq_result_dataset_name=target_bq_result_dataset_name,
                    target_bq_result_table_name=target_bq_result_table_name,
                    task_service_account=dataplex_task_service_account_name,
                    task_trigger_spec_type=task_trigger_spec_type,
                    task_description=task_description,
                    task_labels=task_labels,
                    clouddq_executable_path=clouddq_executable_path,
                    clouddq_executable_checksum_path=clouddq_executable_checksum_path,
                    clouddq_pyspark_driver_path=clouddq_pyspark_driver_path,
                    validate_only=validate_only)
        # Check that the task has been created successfully
        print(f"CloudDQ task creation response is {response.text}")
        assert response.status_code == 200
        # Poll task status until it has either succeeded or failed
        # Tests will timeout if exceeding the duration `--test_timeout` set in `.bazelrc`
        task_status = test_dq_dataplex_client.get_clouddq_task_status(task_id)
        while (task_status != 'SUCCEEDED' and task_status != 'FAILED'):
            print(time.ctime())
            time.sleep(30)
            task_status = test_dq_dataplex_client.get_clouddq_task_status(task_id)
            print(f"CloudDQ task status is {task_status}")
        assert task_status == expected

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
