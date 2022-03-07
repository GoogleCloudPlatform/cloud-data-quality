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

from datetime import date
from datetime import timedelta
from pathlib import Path
from pprint import pformat

import fileinput
import json
import logging
import os
import shutil
import sys

from google.cloud import bigquery

import pytest

from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.main import main
from clouddq.runners.dbt.dbt_runner import DbtRunner
from clouddq.runners.dbt.dbt_utils import get_dbt_invocation_id
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

class TestDqAdvancedRules:

    @pytest.fixture(scope="session")
    def client(self):
        """Get BigQuery Client using discovered ADC"""
        client = BigQueryClient()
        yield client
        client.close_connection()

    @pytest.fixture(scope="session")
    def prepare_test_input_data(
        self,
        client,
        test_data,
        gcp_project_id,
        gcp_dataplex_bigquery_dataset_id
    ):
        input_files_csv = ["accuracy_check_distribution_not_ok.csv", "accuracy_check_distribution_ok.csv",
            "accuracy_check_simple.csv", "completeness_check_not_ok.csv", "completeness_check_ok.csv",
            "conformity_check_not_ok.csv", "conformity_check_ok.csv", "different_volumes_per_period.csv",
            "ingestion_day_level.csv", "ingestion_month_level.csv", "ingestion_timestamp_level.csv",
            "reference_check_not_ok.csv", "reference_check_ok.csv", "reference_data.csv", "reference_data_subquery.csv",
            "reference_data_subquery2.csv", "uniqueness_check_not_ok.csv", "uniqueness_check_ok.csv"]

        input_files_json = ["complex_rules_not_ok.json", "complex_rules_ok.json",
            "reference_check_subquery2_not_ok.json", "reference_check_subquery2_ok.json",
            "reference_check_subquery_not_ok.json", "reference_check_subquery_ok.json"]

        # check if the data files exist, if not, ignore - data has been loaded offline
        if not os.path.exists(test_data / "advanced_rules"):
            return

        client = client.get_connection()
        # override some dates (for the ingestion check - it's looking backwards from the current date)
        # fileinput supports inline editing, it outputs the stdout to the file
        with fileinput.FileInput(test_data / "advanced_rules/ingestion_day_level.csv",
                inplace=True, backup='.bak') as file:
            day1 = (date.today() - timedelta(days=10)).strftime("%Y-%m-%d")
            day2 = (date.today() - timedelta(days=11)).strftime("%Y-%m-%d")
            for line in file:
                line = line.replace("2021-12-15", day1)
                line = line.replace("2021-12-14", day2)
                print(line, end='')

        with fileinput.FileInput(test_data / "advanced_rules/ingestion_month_level.csv",
                inplace=True, backup='.bak') as file:
            today = date.today()
            first = today.replace(day=1)
            lastMonth = first - timedelta(days=1)
            for line in file:
                line = line.replace("202111", lastMonth.strftime("%Y%m"))
                print(line, end='')

        with fileinput.FileInput(test_data / "advanced_rules/ingestion_timestamp_level.csv",
                inplace=True, backup='.bak') as file:
            today = date.today()
            first = today.replace(day=1)
            lastMonth = first - timedelta(days=1)
            for line in file:
                line = line.replace("2021-12", lastMonth.strftime("%Y-%m"))
                print(line, end='')

        for filename in input_files_csv + input_files_json:
            with open(test_data / f"advanced_rules/{filename}", "rb") as source_file:
                table_id = f"{gcp_project_id}.{gcp_dataplex_bigquery_dataset_id}.{os.path.splitext(filename)[0]}"

                file_format = os.path.splitext(filename)[1][1:]
                job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                    if file_format == "json" else bigquery.SourceFormat.CSV,
                        autodetect=True, write_disposition="WRITE_TRUNCATE")

                job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                job.result()  # Waits for the job to complete.

                table = client.get_table(table_id)  # Make an API request.
                logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    @pytest.fixture(scope="session")
    def create_expected_results_table(
        self,
        client,
        test_resources,
        gcp_project_id,
        target_bq_result_dataset_name,
        target_bq_result_table_name):

        client = client.get_connection()

        table_id = (
            f"{gcp_project_id}.{target_bq_result_dataset_name}."
            f"{target_bq_result_table_name}_advanced_rules_expected"
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("null_count", "INTEGER"),
                bigquery.SchemaField("failed_count", "INTEGER"),
                bigquery.SchemaField("complex_rule_validation_success_flag", "BOOLEAN"),
                bigquery.SchemaField("rows_validated", "INTEGER"),
                bigquery.SchemaField("progress_watermark", "BOOLEAN"),
                bigquery.SchemaField("rule_id", "STRING"),
                bigquery.SchemaField("complex_rule_validation_errors_count", "INTEGER"),
                bigquery.SchemaField("metadata_json_string", "STRING"),
                bigquery.SchemaField("column_id", "STRING"),
                bigquery.SchemaField("dimension", "STRING"),
                bigquery.SchemaField("success_count", "INTEGER"),
                bigquery.SchemaField("rule_binding_id", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE")

        with open(test_resources / "dq_advanced_rules_expected_results.json", "rb") as source_file:
            json_data = json.loads(source_file.read())
            job = client.load_table_from_json(json_data, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
        return table_id

    def test_advanced_dq_rules(
        self,
        runner,
        temp_configs_from_dq_advanced_rules_configs,
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
        prepare_test_input_data,
        source_dq_advanced_rules_configs_path,
        test_resources,
        caplog,
    ):
        caplog.set_level(logging.INFO, logger="clouddq")
        try:
            temp_dir = Path(tmp_path).joinpath("cloud_dq_working_dir")
            temp_dir.mkdir(parents=True)

            with working_directory(temp_dir):
                logger.info(f"test_last_modified_in_dq_summary {gcp_application_credentials}")
                target_table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
                args = [
                    "ALL",
                    f"{temp_configs_from_dq_advanced_rules_configs}",
                    f"--gcp_project_id={gcp_project_id}",
                    f"--gcp_bq_dataset_id={gcp_bq_dataset}",
                    f"--gcp_region_id={gcp_bq_region}",
                    f"--target_bigquery_summary_table={target_table}",
                    ]
                result = runner.invoke(main, args)
                logger.info(result.output)
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
                    logger.warning(
                        "Rows with values not matching the expected "
                        "content in 'tests/resources/dq_advanced_rules_expected_results.csv':"
                    )
                    for row in rows:
                        record = json.loads(str(row[0]))
                        logger.warning(f"\n{pformat(record)}")
                failed_rows = [json.loads(row[0]) for row in rows]
                failed_rows_rule_binding_ids = [row['rule_binding_id'] for row in failed_rows]
                failed_rows_rule_ids = [row['rule_id'] for row in failed_rows]
                with open(test_resources / "dq_advanced_rules_expected_results.json", "rb") as source_file:
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
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto'] + sys.argv[1:]))
