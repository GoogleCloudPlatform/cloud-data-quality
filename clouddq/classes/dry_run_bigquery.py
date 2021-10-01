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

from __future__ import annotations

import logging
import re

from pathlib import Path
from string import Template

import google.auth

from google.api_core.exceptions import NotFound
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery
from google.oauth2 import service_account

from clouddq.classes.dry_run_client import DryRunClient


TARGET_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

CHECK_QUERY = Template(
    """
SELECT
    *
FROM (
    $query_string
) q
"""
)

CREATE_DQ_SUMMARY_TABLE_SQL = Template(
    """
CREATE TABLE $table_name (
    execution_ts TIMESTAMP,
    rule_binding_id STRING,
    rule_id STRING,
    table_id STRING,
    column_id STRING,
    rows_validated INT64,
    success_count INT64,
    success_percentage FLOAT64,
    failed_count INT64,
    failed_percentage FLOAT64,
    null_count INT64,
    null_percentage FLOAT64,
    metadata_json_string STRING,
    configs_hashsum STRING,
    dq_run_id STRING,
    progress_watermark BOOL,
)
"""
)

RE_EXTRACT_TABLE_NAME = ".*Not found: Table (.+?) was not found in.*"


class BigQueryDryRunClient(DryRunClient):
    __client: bigquery.client.Client = None
    __credentials: Credentials = None
    __project_id: str = None

    def __init__(
        self,
        credentials: Credentials = None,
        project_id: str = None,
        gcp_service_account_key_path: Path = None,
        gcp_impersonation_credentials: str = None,
    ) -> BigQueryDryRunClient:
        if credentials:
            self.__credentials = credentials
            if project_id:
                self.__project_id = project_id
            else:
                self.__project_id = credentials.project_id
        elif gcp_service_account_key_path:
            self.__credentials = service_account.Credentials.from_service_account_file(
                filename=gcp_service_account_key_path, scopes=TARGET_SCOPES
            )
            self.__project_id = self.__credentials.project_id
        else:
            self.__credentials, self.__project_id = google.auth.default()
            if gcp_impersonation_credentials:
                target_credentials = impersonated_credentials.Credentials(
                    source_credentials=credentials,
                    target_principal=gcp_impersonation_credentials,
                    target_scopes=TARGET_SCOPES,
                    lifetime=500,
                )
                self.__credentials = target_credentials

    @classmethod
    def get_connection(cls) -> bigquery.client.Client:
        """Creates return new Singleton database connection"""
        if cls.__client is None:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            cls.__client = bigquery.Client(
                default_query_job_config=job_config,
                credentials=cls.__credentials,
                project=cls.__project_id,
            )
            return cls.__client
        else:
            return cls.__client

    @classmethod
    def close_connection(cls) -> None:
        if cls.__client:
            cls.__client.close()

    @classmethod
    def check_query(cls, query_string: str) -> None:
        """check whether query is valid on singleton db connection"""
        client = cls.get_connection()
        query = CHECK_QUERY.safe_substitute(query_string=query_string.strip())
        # Start the query, passing in the extra configuration.
        query_job = None
        try:
            query_job = client.query(query=query, timeout=10)  # Make an API request.
        except NotFound as e:
            table_name = re.search(RE_EXTRACT_TABLE_NAME, str(e))
            if table_name:
                table_name = table_name.group(1).replace(":", ".")
                logging.warning(f"Table name `{table_name}` does not exist.")
            logging.fatal(e)
            raise e
        except GoogleAuthError as e:
            logging.fatal("Error connecting to GCP.")
            raise e
        if query_job:
            # A dry run query completes immediately.
            logging.info(
                "This query will process {} bytes.".format(
                    query_job.total_bytes_processed
                )
            )
