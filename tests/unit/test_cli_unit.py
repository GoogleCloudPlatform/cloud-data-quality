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

from pathlib import Path

import logging

import click.testing
import pytest

from clouddq.main import main


logger = logging.getLogger(__name__)

class TestCli:
    @pytest.fixture(scope="session")
    def runner(self):
        return click.testing.CliRunner()

    @pytest.fixture(scope="session")
    def test_configs_dir(self):
        return Path("tests").joinpath("resources", "configs")

    @pytest.fixture(scope="session")
    def test_profiles_dir(self):
        return Path("tests").joinpath("resources", "test_dbt_profiles_dir")

    def test_cli_no_args_fail(self, runner):
        result = runner.invoke(main)
        logger.info(result.output)
        assert result.exit_code == 2
        assert isinstance(result.exception, SystemExit)

    def test_cli_help_text(self, runner):
        result = runner.invoke(main, ["--help"])
        logger.info(result.output)
        assert result.exit_code == 0

    def test_cli_missing_dbt_profiles_dir_fail(
        self,
        runner,
        test_configs_dir):
        args = [
            "ALL",
            f"{test_configs_dir}",
            "--dry_run",
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        logger.info(result.output)
        assert result.exit_code == 1
        assert isinstance(result.exception, ValueError)

    def test_cli_dry_run(
        self,
        runner,
        test_configs_dir,
        test_profiles_dir):
        args = [
            "ALL",
            f"{test_configs_dir}",
            f"--dbt_profiles_dir={test_profiles_dir}",
            "--dry_run",
            "--debug",
            "--skip_sql_validation"
            ]
        result = runner.invoke(main, args)
        logger.info(result.output)
        assert result.exit_code == 0

    def test_cli_dry_run_incompatiable_arguments_failure(
        self,
        runner,
        test_configs_dir,
        test_profiles_dir,
        gcp_application_credentials,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_bq_region):
        logger.info(f"Running test_cli_dbt_path with {gcp_project_id=}, {gcp_bq_dataset=}, {gcp_bq_region=}")
        logger.info(f"test_cli_dbt_path {gcp_application_credentials=}")
        args = [
            "ALL",
            f"{test_configs_dir}",
            f"--dbt_profiles_dir={test_profiles_dir}",
            f"--gcp_project_id={gcp_project_id}",
            f"--gcp_bq_dataset_id={gcp_bq_dataset}",
            f"--gcp_region_id={gcp_bq_region}",
            f"--target_bigquery_summary_table={gcp_project_id}.{gcp_bq_dataset}.dq_summary_target",
            "--debug",
            "--dry_run",
            "--summary_to_stdout",
            ]
        logger.info(f"Args: {' '.join(args)}")

        result = runner.invoke(main, args)
        print(result.output)
        assert result.exit_code == 1
        assert isinstance(result.exception, ValueError)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n 2']))
