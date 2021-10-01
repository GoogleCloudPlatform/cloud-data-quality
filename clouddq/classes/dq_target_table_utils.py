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

import logging

from datetime import date

import google.auth

from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob
from google.cloud.exceptions import NotFound


logger = logging.getLogger(__name__)

# getting the credentials and project details for gcp project
credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
# creating a big query clients.
bqclient = bigquery.Client(
    credentials=credentials,
    project=your_project_id,
)


def is_tbl_exists(bqclient, table) -> bool:
    try:
        bqclient.get_table(table)
        return True
    except NotFound:
        return False


def execute_query(query_string: str) -> QueryJob:
    """
    The method is used to execute the sql query
    Parameters:
    query_string (str) : sql query to be executed
    Returns:
        result of the sql execution is returned
    """
    try:
        logger.debug("Query string is \n %s", query_string)
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        query_object = bqclient.query(query_string, job_config=job_config).result()
        return query_object

    except Exception as e:
        logger.error(f"Query Execution failed with error {e}\n", exc_info=True)


class TargetTable:
    invocation_id: str = None

    def __init__(self, invocation_id):
        self.invocation_id = invocation_id

    def write_to_target_bq_table(
        self, partition_date: date, target_bigquery_summary_table: str
    ):
        try:

            if is_tbl_exists(bqclient, target_bigquery_summary_table):

                job_config = bigquery.QueryJobConfig(
                    destination=target_bigquery_summary_table,
                    write_disposition="WRITE_APPEND",
                )

                query_string = f"""SELECT * FROM `dataplex-clouddq.dataplex_clouddq.dq_summary` 
                 WHERE invocation_id='{self.invocation_id}' 
                 and DATE(execution_ts) = '{partition_date}'"""

                # Start the query, passing in the extra configuration.
                # Make an API request and wait for the job to complete
                bqclient.query(query_string, job_config=job_config).result()
                logger.info(
                    "Table already exists \n "
                    "and query results loaded to the table {}".format(
                        target_bigquery_summary_table
                    )
                )

            else:

                query_string = f"""CREATE TABLE 
                `{target_bigquery_summary_table}` 
                PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY) 
                CLUSTER BY table_id, column_id, rule_binding_id, rule_id 
                AS 
                SELECT * from `dataplex-clouddq.dataplex_clouddq.dq_summary` 
                WHERE invocation_id='{self.invocation_id}' 
                AND DATE(execution_ts) = '{partition_date}'
                """
                bqclient.query(query_string).result()
                logger.info(
                    "Table created and query results loaded to the table {}".format(
                        target_bigquery_summary_table
                    )
                )

        except Exception as e:

            logger.error(
                f" Target table creation failed with error {e}\n", exc_info=True
            )
