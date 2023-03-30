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

from __future__ import annotations

from pathlib import Path
from string import Template

import logging
import re

from google.api_core.client_info import ClientInfo
from google.api_core.exceptions import Forbidden
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from clouddq.integration import USER_AGENT_TAG


REQUIRED_COLUMN_TYPES = {
    "last_modified": "TIMESTAMP",
    "dimension": "STRING",
    "dataplex_lake": "STRING",
    "dataplex_zone": "STRING",
    "dataplex_asset_id": "STRING",
    "complex_rule_validation_success_flag": "BOOLEAN",
    "failed_records_query": "STRING",
}

from clouddq.integration.gcp_credentials import GcpCredentials


logger = logging.getLogger(__name__)

CHECK_QUERY = Template(
    """
SELECT
    *
FROM (
    $query_string
) q
"""
)

RE_EXTRACT_TABLE_NAME = ".*Not found: Table (.+?) was not found in.*"


class BigQueryClient:
    _gcp_credentials: GcpCredentials
    _client: bigquery.client.Client = None
    target_audience = "https://bigquery.googleapis.com"

    def __init__(
        self,
        gcp_credentials: GcpCredentials = None,
        gcp_project_id: str = None,
        gcp_service_account_key_path: Path = None,
        gcp_impersonation_credentials: str = None,
    ) -> None:
        if gcp_credentials:
            self._gcp_credentials = gcp_credentials
        else:
            self._gcp_credentials = GcpCredentials(
                gcp_project_id=gcp_project_id,
                gcp_service_account_key_path=gcp_service_account_key_path,
                gcp_impersonation_credentials=gcp_impersonation_credentials,
            )

    def get_connection(self, new: bool = False) -> bigquery.client.Client:
        """Creates return new Singleton database connection"""
        if self._client is None or new:
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            self._client = bigquery.Client(
                default_query_job_config=job_config,
                credentials=self._gcp_credentials.credentials,
                project=self._gcp_credentials.project_id,
                client_info=ClientInfo(user_agent=USER_AGENT_TAG),
            )
            return self._client
        else:
            return self._client

    def close_connection(self) -> None:
        if self._client:
            self._client.close()

    def get_dataset_region(self, dataset: str) -> str:
        client = self.get_connection()
        try:
            dataset_info = client.get_dataset(dataset)
        except KeyError as error:
            raise KeyError(f"\n\nInput dataset `{dataset}` is not valid.\n{error}")
        return dataset_info.location

    def check_query_dry_run(self, query_string: str) -> None:
        """check whether query is valid."""
        dry_run_job_config = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False, use_legacy_sql=False
        )
        try:
            client = self.get_connection()
            query = CHECK_QUERY.safe_substitute(query_string=query_string.strip())
            # Start the query, passing in the extra configuration.
            query_job = client.query(
                query=query, timeout=240, job_config=dry_run_job_config
            )
            # A dry run query completes immediately.
            logger.debug(
                "This query will process {} bytes.".format(
                    query_job.total_bytes_processed
                )
            )
        except NotFound as e:
            table_name = re.search(RE_EXTRACT_TABLE_NAME, str(e))
            if table_name:
                table_name = table_name.group(1).replace(":", ".")
                raise AssertionError(f"Table name `{table_name}` does not exist.")
        except Forbidden as e:
            logger.error("User has insufficient permissions.")
            raise e

    def is_table_exists(self, table: str) -> bool:
        try:
            client = self.get_connection()
            client.get_table(table)
            return True
        except (NotFound, KeyError):
            return False

    def assert_required_columns_exist_in_table(self, table: str) -> dict:
        try:
            client = self.get_connection()
            table_ref = client.get_table(table)
            column_names = {column.name for column in table_ref.schema}
            failures = {}
            for column_name, column_type in REQUIRED_COLUMN_TYPES.items():
                if column_name not in column_names:
                    failures[
                        column_name
                    ] = f"ALTER TABLE `{table}` ADD COLUMN {column_name} {column_type};\n"
            if failures:
                logger.info(
                    f"Cannot find required column '{list(failures.keys())}' in BigQuery table '{table}'.\n"
                    f"These will created by running the following SQL script :\n"
                    "```\n" + "\n".join(failures.values()) + "```"
                )
                return failures
        except NotFound:
            logger.warning(
                f"Table {table} does not yet exist. It will be created in this run."
            )
        except KeyError as error:
            logger.fatal(f"Input table `{table}` is not valid.", exc_info=True)
            raise KeyError(f"\n\nInput table `{table}` is not valid.\n{error}")

    def table_from_string(self, full_table_id: str) -> bigquery.table.Table:
        return bigquery.table.Table.from_string(full_table_id)

    def is_dataset_exists(self, dataset: str) -> bool:
        try:
            client = self.get_connection()
            client.get_dataset(dataset)
            return True
        except (NotFound, KeyError):
            return False

    def execute_query(
        self,
        query_string: str,
        job_config: bigquery.job.QueryJobConfig = None,
    ) -> bigquery.job.QueryJob:
        """
        The method is used to execute the sql query
        Parameters:
        query_string (str) : sql query to be executed
        Returns:
            result of the sql execution is returned
        """
        client = self.get_connection()
        logger.debug(f"Query string is:\n```\n{query_string}\n```")
        job_id_prefix = "clouddq-"

        if not job_config:
            job_config = bigquery.QueryJobConfig(
                use_query_cache=False, use_legacy_sql=False
            )

        query_job = client.query(
            query_string, job_config=job_config, job_id_prefix=job_id_prefix
        )

        return query_job

    def get_table_schema(self, table: str) -> dict:

        client = self.get_connection()
        try:
            table_ref = client.get_table(table)
        except KeyError as error:
            raise KeyError(f"\n\nInput table `{table}` is not valid.\n{error}")
        columns = {}
        for column in table_ref.schema:
            column_configs = {
                "name": column.name,
                "type": column.field_type,
                "mode": column.mode,
                "data_type": column.field_type,
            }
            columns[column.name.upper()] = column_configs
        table_type = table_ref.table_type
        logger.debug(f"Table Type is : {table_type}")
        table_partitioning_type = table_ref.partitioning_type
        logger.debug(f"Table Partitioning Type is : {table_partitioning_type}")
        if table_partitioning_type:
            partition_fields = []
            time_partitioning = table_ref.time_partitioning
            if time_partitioning:
                partition_field = {}
                time_partitioning_type = time_partitioning.type_
                time_partitioning_field = time_partitioning.field
                if not time_partitioning_field:
                    partition_field["name"] = "_PARTITIONTIME"
                    partition_field["partitioning_type"] = time_partitioning_type
                    partition_field["type"] = "TIMESTAMP"
                    partition_fields.append(partition_field)
                else:
                    partition_field["name"] = time_partitioning_field
                    partition_field["type"] = columns[time_partitioning_field.upper()][
                        "type"
                    ]
                    partition_field["partitioning_type"] = time_partitioning_type
                    partition_fields.append(partition_field)

            range_partitioning = table_ref.range_partitioning
            if range_partitioning:
                raise NotImplementedError(
                    f"Bigquery Range Partitioning for table '{table}' is "
                    f"currently not supported."
                )
            columns_dict = {
                "columns": columns,
                "partition_fields": partition_fields,
            }
        else:
            columns_dict = {
                "columns": columns,
                "partition_fields": None,
            }

        logger.debug(f"Schema for table {table} is: {columns_dict}")
        return columns_dict

    def get_table_columns(self, table: str) -> set:

        client = self.get_connection()
        try:
            table_ref = client.get_table(table)
        except KeyError as error:
            raise KeyError(f"\n\nInput table `{table}` is not valid.\n{error}")

        column_names = {column.name for column in table_ref.schema}
        logger.debug(f"List of columns in table {table} is: {column_names}")

        return column_names
