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

from clouddq.main import main

@pytest.mark.e2e
class TestCliIntegration:
    @pytest.fixture
    def runner(self):
        return click.testing.CliRunner()

    @pytest.fixture
    def gcp_project_id(self):
        return os.environ.get('GOOGLE_CLOUD_PROJECT', '<your_gcp_project_id>')

    @pytest.fixture
    def gcp_bq_dataset(self):
        return os.environ.get('CLOUDDQ_BIGQUERY_DATASET', 'clouddq')

    @pytest.fixture
    def gcp_bq_region(self):
        return os.environ.get('CLOUDDQ_BIGQUERY_REGION', 'EU')

    @pytest.fixture
    def gcp_sa_key(self):
        sa_key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', None)
        if "application_default_credentials.json" in sa_key_path:
            return None
        else:
            return sa_key_path

    def test_cli_dry_run_oauth_configs(
        self, 
        runner, 
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
    ):
        args = [
            "ALL", 
            "tests/resources/configs", 
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}", 
            f"--gcp_region_id={gcp_bq_region}",
            "--dry_run", 
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_sa_key_configs(
        self, 
        runner, 
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_sa_key
    ):
        args = [
            "ALL", 
            "tests/resources/configs", 
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}", 
            f"--gcp_region_id={gcp_bq_region}",
            f"--gcp_service_account_key_path={gcp_sa_key}",
            "--dry_run", 
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        print(result.output)
        if gcp_sa_key:
            assert result.exit_code == 0
        else:
            assert result.exit_code == 2


    def test_cli_dry_run_incompatible_conn_failure(
        self, 
        runner, 
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
    ):
        args = [
            "ALL", 
            "tests/resources/configs", 
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}", 
            f"--gcp_region_id={gcp_bq_region}",
            f"--gcp_service_account_key_path=tests/resources/expected_configs.json",
            f"--gcp_impersonation_credentials=svc@project.iam.gserviceaccount.com",
            "--dry_run", 
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 1

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
