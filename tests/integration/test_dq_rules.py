# Copyright 2022 Google LLC
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
from pprint import pformat

import json
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
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE")

        with open(test_resources / "dq_rules_expected_results.json", "rb") as source_file:
            json_data = json.loads(source_file.read())
            job = client.load_table_from_json(json_data, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
        return table_id

    def test_dq_rules(
        self,
        runner,
        temp_configs_from_dq_rules_config_file,
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
        source_dq_rules_configs_file_path,
        test_resources,
        caplog,
    ):
        caplog.set_level(logging.INFO, logger="clouddq")
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_dq_rules")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                logger.info(f"test_last_modified_in_dq_summary {gcp_application_credentials}")
                target_table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
                args = [
                    "ALL",
                    f"{temp_configs_from_dq_rules_config_file}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    f"--target_bigquery_summary_table={target_table}",
                    ]
                result = runner.invoke(main, args)
                assert result.exit_code == 0
                intermediate_table_expiration_hours = 24

                num_threads = 8
                # Prepare dbt runtime
                dbt_runner = DbtRunner(
                    environment_target="Dev",
                    gcp_project_id=gcp_project_id,
                    gcp_region_id=gcp_bq_region,
                    gcp_bq_dataset_id=gcp_bq_dataset,
                    gcp_service_account_key_path=gcp_sa_key,
                    gcp_impersonation_credentials=gcp_impersonation_credentials,
                    intermediate_table_expiration_hours=intermediate_table_expiration_hours,
                    num_threads=num_threads,
                )
                dbt_path = dbt_runner.get_dbt_path()
                invocation_id = get_dbt_invocation_id(dbt_path)
                logger.info(f"Dbt invocation id is: {invocation_id}")
                # Test the DQ expected results
                sql = f"""
                WITH validation_errors AS (
                SELECT rule_binding_id, rule_id, column_id,
                dimension, metadata_json_string, progress_watermark,
                rows_validated, complex_rule_validation_errors_count,
                complex_rule_validation_success_flag,
                success_count, failed_count, null_count
                FROM
                    `{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}`
                WHERE
                    invocation_id='{invocation_id}'
                EXCEPT DISTINCT
                SELECT rule_binding_id, rule_id, column_id,
                dimension, metadata_json_string, progress_watermark,
                rows_validated, complex_rule_validation_errors_count,
                complex_rule_validation_success_flag,
                success_count, failed_count, null_count
                FROM
                    `{create_expected_results_table}`
                )
                SELECT TO_JSON_STRING(validation_errors)
                FROM validation_errors;
                """
                logger.info(f"SQL query is: {sql}")
                query_job = client.execute_query(sql)
                results = query_job.result()
                logger.info("Query done")
                rows = list(results)
                logger.info(f"Query execution returned {len(rows)} rows")
                if len(rows):
                    logger.info(f"Input yaml from {source_dq_rules_configs_file_path}:")
                    with open(source_dq_rules_configs_file_path) as input_yaml:
                        lines = input_yaml.read()
                        logger.info(lines)
                    logger.warning(
                        "Rows with values not matching the expected "
                        "content in 'tests/resources/expected_results.json':"
                    )
                    for row in rows:
                        record = json.loads(str(row[0]))
                        logger.warning(f"\n{pformat(record)}")
                failed_rows = [json.loads(row[0]) for row in rows]
                failed_rows_rule_binding_ids = [row['rule_binding_id'] for row in failed_rows]
                failed_rows_rule_ids = [row['rule_id'] for row in failed_rows]
                with open(test_resources / "dq_rules_expected_results.json", "rb") as source_file:
                    expected_json = []
                    json_data = json.loads(source_file.read())
                    for record in json_data:
                        if record['rule_binding_id'] not in failed_rows_rule_binding_ids:
                            continue
                        if record['rule_id'] not in failed_rows_rule_ids:
                            continue
                        expected_json.append(record)
                assert failed_rows == expected_json
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
