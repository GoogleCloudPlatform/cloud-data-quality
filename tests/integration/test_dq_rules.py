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

from google.cloud import bigquery

import pytest

from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.main import main
from clouddq.runners.dbt.dbt_runner import DbtRunner
from clouddq.runners.dbt.dbt_utils import get_dbt_invocation_id
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

class TestDqRules:

    @pytest.fixture(scope="session")
    def client(self):
        """Get BigQuery Client using discovered ADC"""
        client = BigQueryClient()
        yield client
        client.close_connection()

    @pytest.fixture(scope="session")
    def create_expected_results_table(
        self,
        client,
        test_resources,
        gcp_project_id,
        target_bq_result_dataset_name,
        target_bq_result_table_name,):

        client = client.get_connection()

        table_id = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}_expected"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE")

        with open(test_resources / "expected_results.csv", "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    def test_dq_rules(
        self,
        runner,
        temp_configs_dir,
        gcp_application_credentials,
        gcp_project_id,
        gcp_bq_dataset,
        gcp_bq_region,
        target_bq_result_dataset_name,
        target_bq_result_table_name,
        tmp_path,
        client,
        gcp_impersonation_credentials,
        gcp_sa_key,
        create_expected_results_table,
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
                    "--debug",
                    "--summary_to_stdout",
                    ]
                result = runner.invoke(main, args)
                print(result.output)
                assert result.exit_code == 0

                # Prepare dbt runtime
                dbt_runner = DbtRunner(
                    dbt_path=None,
                    dbt_profiles_dir=None,
                    environment_target="Dev",
                    gcp_project_id=gcp_project_id,
                    gcp_region_id=gcp_bq_region,
                    gcp_bq_dataset_id=gcp_bq_dataset,
                    gcp_service_account_key_path=gcp_sa_key,
                    gcp_impersonation_credentials=gcp_impersonation_credentials,
                )
                dbt_path = dbt_runner.get_dbt_path()
                invocation_id = get_dbt_invocation_id(dbt_path)
                print(f"Dbt invocation id is {invocation_id}")
                # Test the DQ expected results
                try:
                    sql = f"""SELECT rule_binding_id, rule_id, column_id,
                    dimension, metadata_json_string, progress_watermark,
                    rows_validated, complex_rule_validation_errors_count,
                    success_count, failed_count, null_count
                     FROM
                     `{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}`
                     WHERE invocation_id='{invocation_id}'
                    EXCEPT DISTINCT
                    SELECT rule_binding_id, rule_id, column_id,
                    dimension, metadata_json_string, progress_watermark,
                    rows_validated, complex_rule_validation_errors_count,
                    success_count, failed_count, null_count
                     FROM `{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}_expected`;
                    """
                    print(f"Dq rules validation query string is \n {sql}")
                    query_job = client.execute_query(sql)
                    results = query_job.result()
                    logger.info("Query done")
                    rows = list(results)
                    print(f"Query execution returned {len(rows)} rows")
                    assert len(rows) == 0
                except Exception as exc:
                    logger.fatal(f'Exception in query: {exc}')
                    assert False
                finally:
                    client.close_connection()
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
