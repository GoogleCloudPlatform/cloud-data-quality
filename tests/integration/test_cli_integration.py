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

import click.testing
import pytest
import logging
import tempfile
import shutil
from pathlib import Path

from clouddq.main import main

logger = logging.getLogger(__name__)

@pytest.mark.e2e
class TestCliIntegration:
    @pytest.fixture
    def runner(self):
        return click.testing.CliRunner()

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
        sa_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', None)
        if not sa_key_path:
            logger.fatal("Required test environment variable GOOGLE_APPLICATION_CREDENTIALS cannot be found. Set this to the exported service account key path used for integration testing.")
        return sa_key_path

    @pytest.fixture
    def gcp_impersonation_credentials(self):
        gcp_impersonation_credentials = os.environ.get('IMPERSONATION_SERVICE_ACCOUNT', None)
        if not gcp_impersonation_credentials:
            logger.fatal("Required test environment variable IMPERSONATION_SERVICE_ACCOUNT cannot be found. Set this to the service account name for impersonation used for integration testing.")
        return gcp_impersonation_credentials

    @pytest.fixture
    def temp_configs_dir(self, gcp_project_id, gcp_bq_dataset):
        source_configs_path = Path("tests").joinpath("resources","configs")
        temp_clouddq_dir = Path(tempfile.gettempdir()).joinpath("clouddq_test", "configs")
        if os.path.exists(temp_clouddq_dir):
            shutil.rmtree(temp_clouddq_dir)
        destination = shutil.copytree(source_configs_path, temp_clouddq_dir)
        test_data = Path(destination).joinpath("entities", "test-data.yml")
        with open(test_data) as source_file:
            lines = source_file.read()
        with open(test_data, "w") as source_file:
            lines = lines.replace("<your_gcp_project_id>", gcp_project_id)
            lines = lines.replace("dq_test", gcp_bq_dataset)
            source_file.write(lines)
        yield destination
        shutil.rmtree(destination)

    @pytest.fixture
    def test_profiles_dir(self):
        return Path("tests").joinpath("resources","test_dbt_profiles_dir")

    def test_cli_dry_run_dbt_path(
        self,
        runner,
        temp_configs_dir,
        test_profiles_dir,
    ):
        args = [
            "ALL", 
            f"{temp_configs_dir}", 
            f"--dbt_profiles_dir={test_profiles_dir}", 
            "--dry_run", 
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_oauth_configs(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_sa_key_configs(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_sa_key
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            f"--gcp_service_account_key_path={gcp_sa_key}",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_sa_key_and_impersonation(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_sa_key,
        gcp_impersonation_credentials,
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            f"--gcp_service_account_key_path={gcp_sa_key}",
            f"--gcp_impersonation_credentials={gcp_impersonation_credentials}",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_oath_impersonation(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_impersonation_credentials,
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            f"--gcp_impersonation_credentials={gcp_impersonation_credentials}",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_oath_impersonation_fail(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            f"--gcp_impersonation_credentials=non-existent-svc@non-existent-project.com",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 1
        assert "Could not get refreshed credentials for GCP." in result.output

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
