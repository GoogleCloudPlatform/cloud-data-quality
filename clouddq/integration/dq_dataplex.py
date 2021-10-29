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

import os

import json
import logging
import time

import google.auth
import google.auth.transport.requests

from google.auth.credentials import Credentials
from requests import Response
from requests import Session
from requests_oauth2 import OAuth2BearerToken

from clouddq.integration.dataplex_client import DataplexClient


logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        dataplex_endpoint,
        gcp_project_id,
        location_id,
        lake_name,
        gcp_bucket_name,
        gcp_bq_dataset,
        gcp_bq_region,
    ):
        self.dataplex_endpoint = dataplex_endpoint
        self.gcp_project_id = gcp_project_id
        self.location_id = location_id
        self.lake_name = lake_name
        self.gcp_bucket_name = gcp_bucket_name
        self.gcp_bq_dataset = gcp_bq_dataset
        self.gcp_bq_region = gcp_bq_region
        # self.gcp_bucket_name = f"{gcp_bucket_name}_{location_id}"


    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    def get_auth_token(credentials: Credentials) -> str:
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

    # get auth token
    auth_token = get_auth_token(credentials)

    # create request headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + auth_token,
    }

    def get_session(auth_token: str) -> Session:
        """
        This method create the session object for request
        :return:
        session object
        """
        with Session() as session:
            session.auth = OAuth2BearerToken(auth_token)

        return session

    session = get_session(auth_token)

    def create_clouddq_task(
        self,
        task_id: str,
        trigger_spec_type: str,
        task_description: str,
        data_quality_spec_file_path: str,
        result_dataset_name: str,
        result_table_name: str,
    ) -> Response:

        default_body = {
            "spark": {
                "python_script_file": f"gs://{self.gcp_bucket_name}/"
                f"clouddq_pyspark_driver.py",
                "archive_uris": [data_quality_spec_file_path],
                "file_uris": [
                    f"gs://{self.gcp_bucket_name}/clouddq-executable.zip",
                    f"gs://{self.gcp_bucket_name}/clouddq-executable.zip.hashsum",
                ],
            },
            "execution_spec": {
                "args": {
                    "TASK_ARGS": f"clouddq-executable.zip, ALL, configs, "
                    f'--gcp_project_id="{self.gcp_project_id}", '
                    f'--gcp_region_id="{self.gcp_bq_region}", '
                    f'--gcp_bq_dataset_id="{self.gcp_bq_dataset}", '
                    f"--target_bigquery_summary_table="
                    f'"{self.gcp_project_id}.{result_dataset_name}.{result_table_name}",'
                }
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
