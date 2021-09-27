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
        print(runner.invoke(main, ['--help']).output)
        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 0

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
