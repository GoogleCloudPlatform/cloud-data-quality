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

from collections import defaultdict
from pathlib import Path
from pprint import pformat

import datetime
import hashlib
import json
import logging
import shutil
import sys
import time

import pytest
import yaml

from clouddq.integration import clouddq_pyspark_driver
from clouddq.integration import test_clouddq_pyspark_driver
from clouddq.integration.dataplex.clouddq_dataplex import CloudDqDataplexClient
from clouddq.integration.gcs import upload_blob
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

@pytest.mark.dataplex
class TestDataplexIntegration:

    @pytest.fixture
    def configs_archive_path(self, tmp_path):
        configs_archive_path = Path(tmp_path).joinpath("clouddq_test_artifacts", "archives").absolute()
        configs_archive_path.mkdir(parents=True, exist_ok=True)
        return configs_archive_path

    @pytest.fixture
    def gcs_clouddq_configs_standard(self, temp_configs_dir, configs_archive_path, gcs_bucket_name):
        print("Invoked 'gcs_clouddq_configs_standard' fixture.")
        file_name = "clouddq-configs.zip"
        with working_directory(configs_archive_path):
            # Create standard configs zip called 'clouddq-configs.zip'
            shutil.make_archive('clouddq-configs', 'zip', temp_configs_dir.parent, temp_configs_dir.name)
            configs_file_path = configs_archive_path.joinpath(file_name).absolute()
            print(configs_file_path)
            assert configs_file_path.is_file()
            upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
            gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
            print(gcs_uri)
            yield gcs_uri
            if configs_file_path.is_file():
                configs_file_path.unlink()

    @pytest.fixture
    def gcs_clouddq_configs_nonstandard_local(self, temp_configs_dir, configs_archive_path):
        print("Invoked 'gcs_clouddq_configs_nonstandard_local' fixture.")
        file_name = "non-standard-clouddq-configs-local.zip"
        with working_directory(configs_archive_path):
            # Create non-standard configs zip called 'non-standard-clouddq-configs-local.zip'
            shutil.make_archive('non-standard-clouddq-configs-local', 'zip', temp_configs_dir)
            configs_file_path = configs_archive_path.joinpath(file_name).absolute()
            print(configs_file_path)
            assert configs_file_path.is_file()
            yield configs_file_path.absolute()
            if configs_file_path.is_file():
                configs_file_path.unlink()

    @pytest.fixture
    def gcs_clouddq_configs_nonstandard(self, temp_configs_dir, configs_archive_path, gcs_bucket_name):
        print("Invoked 'gcs_clouddq_configs_nonstandard' fixture.")
        file_name = "non-standard-clouddq-configs.zip"
        with working_directory(configs_archive_path):
            # Create non-standard configs zip called 'non-standard-clouddq-configs.zip'
            shutil.make_archive('non-standard-clouddq-configs', 'zip', temp_configs_dir)
            configs_file_path = configs_archive_path.joinpath(file_name).absolute()
            print(configs_file_path)
            assert configs_file_path.is_file()
            upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
            gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
            print(gcs_uri)
            yield gcs_uri
            if configs_file_path.is_file():
                configs_file_path.unlink()

    @pytest.fixture
    def gcs_clouddq_configs_empty(self, configs_archive_path, gcs_bucket_name):
        print("Invoked 'gcs_clouddq_configs_empty' fixture.")
        file_name = "empty-clouddq-configs.zip"
        with working_directory(configs_archive_path):
            # Create empty configs zip called 'empty-clouddq-configs.zip'
            empty_directory = Path(configs_archive_path).joinpath("empty").absolute()
            empty_directory.mkdir(parents=True, exist_ok=True)
            shutil.make_archive('empty-clouddq-configs', 'zip', empty_directory.parent, empty_directory.name)
            configs_file_path = configs_archive_path.joinpath(file_name).absolute()
            print(configs_file_path)
            assert configs_file_path.is_file()
            upload_blob(gcs_bucket_name, configs_file_path, f"test-artifacts/{file_name}")
            gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
            print(gcs_uri)
            yield gcs_uri
            if configs_file_path.joinpath(file_name).is_file():
                configs_file_path.unlink()

    @pytest.fixture
    def temp_configs_files_path(self, temp_configs_dir, tmp_path):
        print("Invoked 'temp_configs_files_path' fixture.")
        configs_files_path = Path(tmp_path).joinpath("clouddq_test_artifacts", "files").absolute()
        configs_files_path.mkdir(parents=True, exist_ok=True)
        with working_directory(configs_files_path):
            # Create single YAML config files for testing
            configs_content = []
            for file in temp_configs_dir.glob("**/*.yml"):
                configs_content.append(file.open().read())
            for file in temp_configs_dir.glob("**/*.yaml"):
                configs_content.append(file.open().read())
            # Create single malformed YAML config called 'malformed-configs.yml'
            # This breaks because there are two nodes for rules: on the same file, meaning one will be ignored
            single_malformed_yaml_path = Path(configs_files_path).joinpath("malformed-configs.yml")
            # only taking entities and ignoring other configs to make it a malformed yaml
            malformed_content = [content for content in configs_content if "entities:" in content]
            single_malformed_yaml_path.write_text("\n".join(malformed_content))
            assert configs_files_path.joinpath("malformed-configs.yml").is_file()
            # Fix the above issue and write to a YAML config called 'configs.yml'
            merged_configs = defaultdict(dict)
            for config in configs_content:
                parsed_content = yaml.safe_load(config)
                for config_type, config_body in parsed_content.items():
                    for config_id, config_item in config_body.items():
                        merged_configs[config_type][config_id] = config_item
            single_yaml_path = Path(configs_files_path).joinpath("configs.yml")
            single_yaml_path.write_text(yaml.safe_dump(dict(merged_configs)))
            assert configs_files_path.joinpath("configs.yml").is_file()
            # Print temp configs path
            print(f"Content of configs file path: {configs_files_path}:")
            print(pformat(list(configs_files_path.glob("**/*.yml"))))
            # Return path and delete when done
            yield configs_files_path.absolute()

    @pytest.fixture
    def gcs_clouddq_configs_single_yaml(self, temp_configs_files_path, gcs_bucket_name):
        # Print temp configs path
        print(f"Uploading 'gcs_clouddq_configs_single_yaml' from: {temp_configs_files_path}:")
        print(pformat(list(temp_configs_files_path.glob("**/*.yml"))))
        # Load to GCS and return GCS path
        file_name = "configs.yml"
        single_yaml_path = temp_configs_files_path.joinpath(file_name)
        assert single_yaml_path.is_file()
        upload_blob(gcs_bucket_name, single_yaml_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        print(gcs_uri)
        yield gcs_uri
        if single_yaml_path.is_file():
            single_yaml_path.unlink()

    @pytest.fixture
    def gcs_clouddq_configs_single_yaml_malformed(self, temp_configs_files_path, gcs_bucket_name):
        # Print temp configs path
        print(f"Uploading 'gcs_clouddq_configs_single_yaml_malformed' from: {temp_configs_files_path}:")
        print(pformat(list(temp_configs_files_path.glob("**/*.yml"))))
        # Load to GCS and return GCS path
        file_name = "malformed-configs.yml"
        single_yaml_malformed_path = temp_configs_files_path.joinpath(file_name)
        assert single_yaml_malformed_path.is_file()
        upload_blob(gcs_bucket_name, single_yaml_malformed_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        print(gcs_uri)
        yield gcs_uri
        if single_yaml_malformed_path.is_file():
            single_yaml_malformed_path.unlink()

    @pytest.fixture(scope="session")
    def gcs_clouddq_pyspark_driver(self, gcs_bucket_name):
        file_name = 'clouddq_pyspark_driver.py'
        driver_path = clouddq_pyspark_driver.__file__
        upload_blob(gcs_bucket_name, driver_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        print(gcs_uri)
        return gcs_uri

    @pytest.fixture(scope="session")
    def test_gcs_clouddq_pyspark_driver(self, gcs_bucket_name):
        file_name = 'test_clouddq_pyspark_driver.py'
        driver_path = test_clouddq_pyspark_driver.__file__
        upload_blob(gcs_bucket_name, driver_path, f"test-artifacts/{file_name}")
        gcs_uri = f"gs://{gcs_bucket_name}/test-artifacts/{file_name}"
        print(gcs_uri)
        return gcs_uri

    @pytest.fixture(scope="session")
    def test_clouddq_zip_executable_paths(self,
                                gcs_bucket_name,
                                gcs_clouddq_executable_path):
        if gcs_clouddq_executable_path:
            gcs_zip_executable_name = f"{gcs_clouddq_executable_path}/clouddq-executable.zip"
            gcs_zip_executable_hashsum_name = f"{gcs_clouddq_executable_path}/clouddq-executable.zip.hashsum"
        else:
            clouddq_zip_build = Path("clouddq_patched.zip")
            if not clouddq_zip_build.is_file():
                raise RuntimeError(
                    f"Local CloudDQ Artifact Zip at `{clouddq_zip_build}` "
                    "not found. Run `make build` before continuing.")
            upload_blob(gcs_bucket_name, clouddq_zip_build, "test-artifacts/clouddq-executable.zip")
            gcs_zip_executable_name = f"gs://{gcs_bucket_name}/test-artifacts/clouddq-executable.zip"
            print(gcs_zip_executable_name)
            hash_sha256 = hashlib.sha256()
            with open(clouddq_zip_build, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            hashsum = hash_sha256.hexdigest()
            hashsum_file = Path("clouddq_patched.zip.hashsum")
            hashsum_file.write_text(hashsum)
            upload_blob(gcs_bucket_name, hashsum_file, "test-artifacts/clouddq-executable.zip.hashsum")
            gcs_zip_executable_hashsum_name = f"gs://{gcs_bucket_name}/test-artifacts/clouddq-executable.zip.hashsum"
            print(gcs_zip_executable_hashsum_name)
        return (gcs_zip_executable_name, gcs_zip_executable_hashsum_name)

    @pytest.fixture(scope="session")
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
                                gcp_dataplex_lake_name,
                                request):
        print(f"Running Dataplex integration test: {request.config.getoption('--run-dataplex')}")
        response = test_dq_dataplex_client.get_dataplex_lake(gcp_dataplex_lake_name)
        assert response.status_code == 200
        resp_obj = json.loads(response.text)
        print(resp_obj)
        expected_name = f"projects/{gcp_project_id}/locations/{gcp_dataplex_region}/lakes/{gcp_dataplex_lake_name}"
        assert resp_obj["name"] == expected_name

    @pytest.mark.parametrize(
        "input_configs,expected,test_driver",
        [
            pytest.param(
                'gcs_clouddq_configs_standard',
                "SUCCEEDED",
                True,
                id="without_clouddq_executable"
            ),
            pytest.param(
                'gcs_clouddq_configs_standard',
                "SUCCEEDED",
                False,
                id="configs_standard"
            ),
            pytest.param(
                'gcs_clouddq_configs_nonstandard',
                "SUCCEEDED",
                False,
                id="configs_nonstandard"
            ),
            pytest.param(
                'gcs_clouddq_configs_nonstandard_local',
                "SUCCEEDED",
                False,
                id="configs_nonstandard_local"
            ),
            pytest.param(
                'gcs_clouddq_configs_empty',
                "FAILED",
                False,
                id="configs_empty"
            ),
            pytest.param(
                'gcs_clouddq_configs_single_yaml',
                "SUCCEEDED",
                False,
                id="configs_single_yaml"
            ),
            pytest.param(
                'gcs_clouddq_configs_single_yaml_malformed',
                "FAILED",
                False,
                id="configs_single_yaml_malformed"
            ),
        ],
    )
    def test_create_bq_dataplex_task(self,
                    test_dq_dataplex_client,
                    input_configs,
                    test_clouddq_zip_executable_paths,
                    expected,
                    test_driver,
                    gcs_clouddq_pyspark_driver,
                    test_gcs_clouddq_pyspark_driver,
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
        print(f"Running Dataplex integration test: {request.config.getoption('--run-dataplex')}")
        test_id = f"{request.node.callspec.id}".replace("_", "-")
        print(f"Test_ID: {test_id}")
        # Use the most updated clouddq_pyspark_driver file from the project for testing
        if test_driver:
            clouddq_pyspark_driver_path: str = test_gcs_clouddq_pyspark_driver
            clouddq_pyspark_driver_filename: str = "test_clouddq_pyspark_driver.py"
            clouddq_executable_path = None
            clouddq_executable_checksum_path = None
            clouddq_yaml_spec_file_path = "gs://dataplex-clouddq-api-integration/test-artifacts/clouddq-configs.zip"
        else:
            # Get executable path
            print(f"Using executable paths: {test_clouddq_zip_executable_paths}")
            clouddq_executable_path, clouddq_executable_checksum_path = test_clouddq_zip_executable_paths
            clouddq_pyspark_driver_path: str = gcs_clouddq_pyspark_driver
            clouddq_pyspark_driver_filename: str = "clouddq_pyspark_driver.py"
            # Prepare the test YAML configurations from fixture
            clouddq_yaml_spec_file_path: str = request.getfixturevalue(input_configs)
            print(f"Using test GCP configs: {clouddq_yaml_spec_file_path}")
        # Prepare Task_ID for reference
        task_id = f"clouddq-test-create-dataplex-task-{test_id}"
        print(f"Dataplex batches task id is: {task_id}")
        # Clean-up old task_ids if exists
        print("Delete task_id if it already exists...")
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
                    clouddq_pyspark_driver_filename=clouddq_pyspark_driver_filename,)
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
    raise SystemExit(pytest.main([__file__, '-v', '-rP', '-n 3'] + sys.argv[1:]))
