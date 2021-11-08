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

import click.testing
import pytest
import logging
from pathlib import Path

from google.auth.exceptions import RefreshError

from clouddq.main import main

logger = logging.getLogger(__name__)

@pytest.mark.e2e
class TestCliIntegration:

    @pytest.fixture
    def runner(self):
        return click.testing.CliRunner()

    @pytest.fixture
    def temp_configs_dir(self, temp_clouddq_dir):
        return Path(temp_clouddq_dir).joinpath("configs")

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

    @pytest.mark.xfail
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
        if not gcp_sa_key:
            pytest.skip("Skipping tests involving exported service-account key credentials because test environment variable GOOGLE_SDK_CREDENTIALS cannot be found.")
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    @pytest.mark.xfail
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
        if not gcp_sa_key:
            pytest.skip("Skipping tests involving exported service-account key credentials because test environment variable GOOGLE_SDK_CREDENTIALS cannot be found.")
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    @pytest.mark.xfail
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
        if not gcp_impersonation_credentials:
            pytest.skip("Skipping tests involving service-account impersonation because test environment variable IMPERSONATION_SERVICE_ACCOUNT cannot be found.")
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
        assert isinstance(result.exception, RefreshError)

    def test_cli_dry_run_missing_project_id_fail(
        self,
        runner,
        temp_configs_dir,
        gcp_bq_region,
        gcp_bq_dataset,
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 1
        assert isinstance(result.exception, ValueError)

    def test_cli_dry_run_dataset_in_wrong_region_fail(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_dataset,
    ):
        args = [
            "ALL",
            f"{temp_configs_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            "--gcp_region_id=nonexistentregion",
            "--dry_run",
            "--debug",
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 1
        assert isinstance(result.exception, AssertionError)

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
