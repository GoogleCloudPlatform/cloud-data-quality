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
from string import Template

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from clouddq.classes.dry_run_client import DryRunClient


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
    @classmethod
    def get_connection(cls, new=False):
        """Creates return new Singleton database connection"""
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        return bigquery.Client(default_query_job_config=job_config)

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
                logging.warning(f"""Table name `{table_name}` does not exist.""")
                logging.fatal(e)
            else:
                logging.fatal(e)
        if query_job:
            # A dry run query completes immediately.
            logging.info(
                "This query will process {} bytes.".format(
                    query_job.total_bytes_processed
                )
            )
