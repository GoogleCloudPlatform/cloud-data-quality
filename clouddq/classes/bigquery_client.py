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
import google.auth.transport.requests

from google.api_core.exceptions import Forbidden
from google.api_core.exceptions import NotFound
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.cloud import bigquery
from google.oauth2 import id_token
from google.oauth2 import service_account


logger = logging.getLogger(__name__)

TARGET_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]

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
    __client: bigquery.client.Client = None
    __credentials: Credentials = None
    __project_id: str = None
    __user_id: str = None
    target_audience = "https://bigquery.googleapis.com"

    def __init__(
        self,
        credentials: Credentials = None,
        project_id: str = None,
        gcp_service_account_key_path: Path = None,
        gcp_impersonation_credentials: str = None,
    ) -> None:
        # Use Credentials object directly if provided
        if credentials:
            source_credentials = credentials
        # Use service account json key if provided
        elif gcp_service_account_key_path:
            source_credentials = service_account.Credentials.from_service_account_file(
                filename=gcp_service_account_key_path,
                scopes=TARGET_SCOPES,
                quota_project_id=project_id,
            )
        # Otherwise, use Application Default Credentials
        else:
            source_credentials, _ = google.auth.default(
                scopes=TARGET_SCOPES, quota_project_id=project_id
            )
        # Attempt to refresh token if not currently valid
        if not source_credentials.valid:
            auth_req = google.auth.transport.requests.Request()
            source_credentials.refresh(auth_req)
        # Attempt service account impersonation if requested
        if gcp_impersonation_credentials:
            target_credentials = impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=gcp_impersonation_credentials,
                target_scopes=TARGET_SCOPES,
                lifetime=3600,
            )
            self.__credentials = target_credentials
        else:
            # Otherwise use source_credentials
            self.__credentials = source_credentials
        self.__project_id = self.__resolve_project_id(
            credentials=self.__credentials, project_id=project_id
        )
        self.__user_id = self.__resolve_credentials_username(
            credentials=self.__credentials
        )
        if self.__user_id:
            logger.info("Successfully created BigQuery Client.")
        else:
            logger.warning(
                "Encountered error while retrieving user from GCP credentials.",
            )

    def __resolve_credentials_username(self, credentials: Credentials) -> str:
        # Attempt to refresh token
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        # Try to get service account credentials user_id
        if credentials.__dict__.get("_service_account_email"):
            user_id = credentials.service_account_email
        elif credentials.__dict__.get("_target_principal"):
            user_id = credentials.service_account_email
        else:
            # Otherwise try to get ADC credentials user_id
            request = google.auth.transport.requests.Request()
            token = credentials.id_token
            id_info = id_token.verify_oauth2_token(token, request)
            user_id = id_info["email"]
        return user_id

    def __resolve_project_id(
        self, credentials: Credentials, project_id: str = None
    ) -> str:
        """Get project ID from local configs"""
        if project_id:
            _project_id = project_id
        elif credentials.__dict__.get("_project_id"):
            _project_id = credentials.project_id
        else:
            logger.warn(
                "Could not retrieve project_id from GCP credentials.", exc_info=True
            )
        return _project_id

    def get_connection(self, new: bool = False) -> bigquery.client.Client:
        """Creates return new Singleton database connection"""
        if self.__client is None or new:
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
            self.__client = bigquery.Client(
                default_query_job_config=job_config,
                credentials=self.__credentials,
                project=self.__project_id,
            )
            return self.__client
        else:
            return self.__client

    def close_connection(self) -> None:
        if self.__client:
            self.__client.close()

    def assert_dataset_is_in_region(self, dataset: str, region: str) -> None:
        client = self.get_connection()
        dataset_info = client.get_dataset(dataset)
        if dataset_info.location != region:
            raise AssertionError(
                f"GCP region for BogQuery jobs in argument --gcp_region_id: "
                f"'{region}' must be the same as dataset '{dataset}' location: "
                f"'{dataset_info.location}'."
            )

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
                query=query, timeout=10, job_config=dry_run_job_config
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
            logger.error(f"User has insufficient permissions.")
            raise e
