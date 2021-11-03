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

import json
import logging
import time

from pathlib import Path

import google.auth
import google.auth.transport.requests

from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.auth.exceptions import RefreshError
from google.oauth2 import id_token
from google.oauth2 import service_account
from requests import Response
from requests import Session
from requests_oauth2 import OAuth2BearerToken

from clouddq.integration.dataplex_client import DataplexClient


logger = logging.getLogger(__name__)
TARGET_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]


class CloudDqDataplex:
    dataplex_endpoint: str = "https://dataplex.googleapis.com"
    gcp_project_id: str
    location_id: str
    lake_name: str
    headers: dict
    session: Session
    auth_token: str
    gcp_bq_dataset = str
    gcp_bq_region = str
    gcp_bucket_name: str = "dataplex-clouddq-api-integration"
    __credentials: Credentials = None
    __user_id: str = None

    def __init__(
        self,
        dataplex_endpoint,
        gcp_project_id,
        location_id,
        lake_name,
        gcp_bucket_name,
        gcp_bq_dataset,
        gcp_bq_region,
        credentials: Credentials = None,
        gcp_service_account_key_path: Path = None,
        gcp_impersonation_credentials: str = None,
    ) -> None:
        self.dataplex_endpoint = dataplex_endpoint
        self.gcp_project_id = gcp_project_id
        self.location_id = location_id
        self.lake_name = lake_name
        self.gcp_bucket_name = gcp_bucket_name
        self.gcp_bq_dataset = gcp_bq_dataset
        self.gcp_bq_region = gcp_bq_region
        # self.gcp_bucket_name = f"{gcp_bucket_name}_{location_id}"

        # getting the credentials and project details for gcp project
        # credentials, your_project_id = google.auth.default(
        #     scopes=TARGET_SCOPES
        # )

        # Use Credentials object directly if provided
        if credentials:
            source_credentials = credentials
        # Use service account json key if provided
        elif gcp_service_account_key_path:
            source_credentials = service_account.Credentials.from_service_account_file(
                filename=gcp_service_account_key_path,
                scopes=TARGET_SCOPES,
                quota_project_id=gcp_project_id,
            )
        # Otherwise, use Application Default Credentials
        else:
            source_credentials, _ = google.auth.default(
                scopes=TARGET_SCOPES, quota_project_id=gcp_project_id
            )
        if not source_credentials.valid:
            self.__refresh_credentials(source_credentials)
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
            credentials=self.__credentials, project_id=gcp_project_id
        )
        self.__user_id = self.__resolve_credentials_username(
            credentials=self.__credentials
        )
        self.auth_token = self.get_auth_token(credentials=self.__credentials)
        self.headers = self.set_headers()
        self.session = self.get_session()
        if self.__user_id:
            logger.info("Successfully created Dataplex Client.")
        else:
            logger.warning(
                "Encountered error while retrieving user from GCP credentials.",
            )

    def __refresh_credentials(self, credentials: Credentials) -> str:
        # Attempt to refresh token if not currently valid
        try:
            auth_req = google.auth.transport.requests.Request()
            credentials.refresh(auth_req)
        except RefreshError as err:
            logger.error("Could not get refreshed credentials for GCP.")
            raise err

    def __resolve_credentials_username(self, credentials: Credentials) -> str:
        # Attempt to refresh token if not currently valid
        if not credentials.valid:
            self.__refresh_credentials(credentials=credentials)
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
            _project_id = None
            logger.warning(
                "Could not retrieve project_id from GCP credentials.", exc_info=True
            )
        return _project_id

    def get_auth_token(self, credentials: Credentials) -> str:
        """
        This method is used to get the authentication token.

        Returns:
        auth_token (str):
        """

        # getting request object
        auth_req = google.auth.transport.requests.Request()

        logger.debug(credentials.valid)  # logger.debugs False
        credentials.refresh(auth_req)  # refresh token
        # check for valid credentials
        logger.debug(credentials.valid)  # logger.debugs True
        auth_token = credentials.token
        logger.debug(auth_token)

        return auth_token

    def set_headers(self) -> dict:
        # create request headers
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.auth_token,
        }

        return headers

    def get_session(self) -> Session:
        """
        This method create the session object for request
        :return:
        session object
        """
        with Session() as session:
            session.auth = OAuth2BearerToken(self.auth_token)

        return session

    def create_clouddq_task(
        self,
        task_id: str,
        trigger_spec_type: str,
        task_description: str,
        data_quality_spec_file_path: str,
        result_dataset_name: str,
        result_table_name: str,
        service_account: str,
        labels: dict,
    ) -> Response:

        default_body = {
            "spark": {
                "python_script_file": f"gs://{self.gcp_bucket_name}/"
                f"empty_clouddq_pyspark_driver.py",
                # "archive_uris": [data_quality_spec_file_path],
                "file_uris": [
                    f"gs://{self.gcp_bucket_name}/clouddq-executable.zip",
                    f"gs://{self.gcp_bucket_name}/clouddq-executable.zip.hashsum",
                    f"{data_quality_spec_file_path}",
                ],
            },
            "execution_spec": {
                "args": {
                    "TASK_ARGS": f"clouddq-executable.zip, ALL, "
                    f"{data_quality_spec_file_path}, "
                    f'--gcp_project_id="{self.gcp_project_id}", '
                    f'--gcp_region_id="{self.gcp_bq_region}", '
                    f'--gcp_bq_dataset_id="{self.gcp_bq_dataset}", '
                    f"--target_bigquery_summary_table="
                    f'"{self.gcp_project_id}.'
                    f'{result_dataset_name}.{result_table_name}",'
                },
                "service_account": f"{service_account}",
            },
            "trigger_spec": {"type": trigger_spec_type},
            "description": task_description,
        }

        response = DataplexClient.create_dataplex_task(
            self,
            dataplex_endpoint=self.dataplex_endpoint,
            gcp_project_id=self.gcp_project_id,
            location_id=self.location_id,
            lake_name=self.lake_name,
            task_id=task_id,
            session=self.session,
            headers=self.headers,
            body=default_body,
        )

        return response

    def get_clouddq_task_jobs(self, task_id: str) -> Response:

        """
        List the dataplex task jobs
        :param task_id: task id for dataplex task
        :return: Response object
        """

        response = DataplexClient.get_dataplex_task_jobs(
            self,
            dataplex_endpoint=self.dataplex_endpoint,
            gcp_project_id=self.gcp_project_id,
            location_id=self.location_id,
            lake_name=self.lake_name,
            task_id=task_id,
            session=self.session,
            headers=self.headers,
        )

        return response

    def get_clouddq_task_status(self, task_id: str) -> str:

        """
        Get the dataplex task status
        :param task_id: dataplex task id
        :return: Task status
        """
        try:
            res = self.get_clouddq_task_jobs(task_id)
            logger.debug(f"Response status code is {res.status_code}")
            logger.debug(f"Response text is {res.text}")
            resp_obj = json.loads(res.text)

            if res.status_code == 200:

                if (
                    "jobs" in resp_obj
                    and len(resp_obj["jobs"]) > 0
                    and "state" in resp_obj["jobs"][0]
                ):
                    task_status = resp_obj["jobs"][0]["state"]
                    return task_status
                else:
                    time.sleep(60)
                    self.get_clouddq_task_status(task_id)

        except Exception as e:
            raise e

    def get_iam_permissions(self) -> list:

        body = {"resource": "dataplex", "permissions": ["roles/dataproc.worker"]}
        return DataplexClient.get_iam_permissions(
            self,
            dataplex_endpoint=self.dataplex_endpoint,
            gcp_project_id=self.gcp_project_id,
            location_id=self.location_id,
            lake_name=self.lake_name,
            session=self.session,
            headers=self.headers,
            body=body,
        )
