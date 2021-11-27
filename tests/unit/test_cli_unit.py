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
import shutil

import click.testing
import pytest

from clouddq.main import main
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

class TestCli:
    @pytest.fixture(scope="session")
    def runner(self):
        return click.testing.CliRunner()

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
        tmp_path,
        source_configs_path):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_dry_run_1")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{source_configs_path}",
                    "--dry_run",
                    "--debug",
                    "--skip_sql_validation"
                    ]
                result = runner.invoke(main, args)
                logger.info(result.output)
                assert result.exit_code == 1
                assert isinstance(result.exception, ValueError)
        finally:
            shutil.rmtree(temp_dir)

    def test_cli_dry_run(
        self,
        runner,
        tmp_path,
        source_configs_path,
        test_profiles_dir):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_dry_run_2")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{source_configs_path}",
                    f"--dbt_profiles_dir={test_profiles_dir}",
                    "--dry_run",
                    "--debug",
                    "--skip_sql_validation"
                    ]
                result = runner.invoke(main, args)
                logger.info(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)

    def test_cli_dry_run_incompatiable_arguments_failure(
        self,
        runner,
        tmp_path,
        source_configs_path,
        test_profiles_dir,
        gcp_application_credentials,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_bq_region):
        logger.info(f"Running test_cli_dbt_path with {gcp_project_id=}, {gcp_bq_dataset=}, {gcp_bq_region=}")
        logger.info(f"test_cli_dbt_path {gcp_application_credentials=}")
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_dry_run_3")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{source_configs_path}",
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
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
