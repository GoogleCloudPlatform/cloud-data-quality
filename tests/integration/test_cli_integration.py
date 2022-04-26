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

import pytest

from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.main import main
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

@pytest.mark.e2e
class TestCliIntegration:

    def test_cli(
        self,
        runner,
        temp_configs_dir,
        gcp_application_credentials,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_bq_region,
        target_bq_result_dataset_name,
        target_bq_result_table_name,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_1")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"Running test_cli_dbt_path with {gcp_project_id}, {gcp_bq_dataset}, {gcp_bq_region}")
                logger.info(f"test_cli_dbt_path {gcp_application_credentials}")
                target_table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{temp_configs_dir}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    f"--target_bigquery_summary_table={target_table}",
                    "--debug",
                    "--summary_to_stdout",
                    ]
                logger.info(f"Args: {' '.join(args)}")

                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)

    def test_cli_dry_run_oauth_configs(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_3")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_oauth_configs {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
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
        finally:
            shutil.rmtree(temp_dir)

    def test_last_modified_in_dq_summary(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_application_credentials,
        tmp_path,
        target_bq_result_dataset_name,
        target_bq_result_table_name,
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_4")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_last_modified_in_dq_summary {gcp_application_credentials}")
                target_table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{temp_configs_dir}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    f"--target_bigquery_summary_table={target_table}",
                    "--debug"
                    ]
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0

                # Test the last modified column in the summary
                try:
                    client = BigQueryClient()
                    sql = f"""
                        WITH last_mod AS (
                            SELECT
                                project_id || '.' || dataset_id || '.' || table_id AS full_table_id,
                                TIMESTAMP_MILLIS(last_modified_time) AS last_modified
                            FROM `{gcp_project_id}.{gcp_bq_dataset}.__TABLES__`
                        )

                        SELECT count(*) as errors
                        FROM `{target_table}` d
                        JOIN last_mod ON last_mod.full_table_id = d.table_id
                        WHERE d.last_modified IS NOT NULL
                            AND NOT d.last_modified = last_mod.last_modified
                    """
                    query_job = client.execute_query(sql)
                    results = query_job.result()
                    logger.info("Query done")
                    row = results.next()
                    errors = row.errors
                    logger.info(f"Got {errors} errors")
                    assert errors == 0
                except Exception as exc:
                    logger.fatal(f'Exception in query: {exc}')
                    assert False
                finally:
                    client.close_connection()
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.xfail
    def test_cli_dry_run_sa_key_configs(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_sa_key,
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_5")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_sa_key_configs {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{temp_configs_dir}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    f"--gcp_service_account_key_path={gcp_sa_key}",
                    "--dry_run",
                    "--debug",
                    ]
                if not gcp_sa_key:
                    pytest.skip("Skipping tests involving exported service-account key "
                    "credentials because test environment variable GOOGLE_SDK_CREDENTIALS "
                    "cannot be found.")
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)

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
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_6")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_sa_key_and_impersonation {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
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
                    pytest.skip("Skipping tests involving exported service-account key "
                    "credentials because test environment variable GOOGLE_SDK_CREDENTIALS"
                    " cannot be found.")
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.xfail
    def test_cli_dry_run_oath_impersonation(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_impersonation_credentials,
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_7")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_oath_impersonation {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{temp_configs_dir}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    f"--gcp_impersonation_credentials={gcp_impersonation_credentials}",
                    "--dry_run",
                    "--debug",
                    ]
                if not gcp_impersonation_credentials:
                    pytest.skip("Skipping tests involving service-account impersonation because "
                    "test environment variable IMPERSONATION_SERVICE_ACCOUNT cannot be found.")
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.xfail
    def test_cli_dry_run_oath_impersonation_fail(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_application_credentials,
        gcp_impersonation_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_8")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_oath_impersonation {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{temp_configs_dir}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    "--gcp_impersonation_credentials=non-existent-svc@non-existent-project.com",
                    "--dry_run",
                    "--debug",
                    ]
                if not gcp_impersonation_credentials:
                    pytest.skip("Skipping tests involving service-account impersonation because "
                    "test environment variable IMPERSONATION_SERVICE_ACCOUNT cannot be found.")
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 1
                assert isinstance(result.exception, SystemExit)
        finally:
            shutil.rmtree(temp_dir)

    def test_cli_dry_run_missing_project_id_fail(
        self,
        runner,
        temp_configs_dir,
        gcp_bq_region,
        gcp_bq_dataset,
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_9")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_missing_project_id_fail {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
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
        finally:
            shutil.rmtree(temp_dir)

    def test_cli_dry_run_dataset_in_wrong_region_fail(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_10")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_dataset_in_wrong_region_fail {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
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
                assert isinstance(result.exception, SystemExit)
        finally:
            shutil.rmtree(temp_dir)

    def test_cli_dry_run_implicit_region_id(
        self,
        runner,
        temp_configs_dir,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_application_credentials,
        tmp_path
    ):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_cli_integration_11")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_cli_dry_run_implicit_region_id {gcp_application_credentials}")
                args = [
                    "T1_DQ_1_VALUE_NOT_NULL,T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE",
                    f"{temp_configs_dir}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    "--dry_run",
                    "--debug",
                    ]
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
