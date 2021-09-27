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
import traceback

import google.auth
from google.auth.credentials import Credentials
import google.auth.transport.requests
from requests import Response
from requests import Session
from requests_oauth2 import OAuth2BearerToken

from clouddq.integration.dataplex_client import DataplexClient


# Create and configure logger
logging.basicConfig(format="%(asctime)s %(message)s")
# Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


class CloudDqDataplex:
    dataplex_endpoint: str = "https://dataplex.googleapis.com"
    project_id: str
    location_id: str
    lake_name: str
    headers: dict
    session: Session
    auth_token: str

    def __init__(self, dataplex_endpoint, project_id, location_id, lake_name):
        self.dataplex_endpoint = dataplex_endpoint
        self.project_id = project_id
        self.location_id = location_id
        self.lake_name = lake_name

    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    def getAuthToken(credentials: Credentials) -> str:
        """
        This method is used to get the authentication token.

        Returns:
        auth_token (str):
        """

        # getting request object
        auth_req = google.auth.transport.requests.Request()

        logger.info(credentials.valid)  # logger.debugs False
        credentials.refresh(auth_req)  # refresh token
        # check for valid credentials
        logger.info(credentials.valid)  # logger.debugs True
        auth_token = credentials.token
        logger.info(auth_token)

        return auth_token

    # get auth token
    auth_token = getAuthToken(credentials)

    # create request headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + auth_token,
    }

    def getSession(auth_token: str) -> Session:
        """
        This method create the session object for request
        :return:
        session object
        """
        with Session() as session:
            session.auth = OAuth2BearerToken(auth_token)

        return session

    session = getSession(auth_token)

    def createCloudDQTask(self, task_id: str, body: dict) -> Response:

        """
        Creates dataplex task
        :param task_id: task id for dataplex task
        :param body: request body for the task
        :return: Response object
        """

        default_body = {
            "spark": {
                "python_script": f'{"gs://dataplex-clouddq-api-integration-test/"}'
                f'{"clouddq_pyspark_driver.py"}',
                "archive_uris": [
                    "gs://dataplex-clouddq-api-integration-test/clouddq-configs.zip"
                ],
                "file_uris": [
                    "gs://dataplex-clouddq-api-integration-test/clouddq_patched.zip",
                    f'{"gs://dataplex-clouddq-api-integration-test/"}'
                    f'{"clouddq_patched.zip.hashsum"}',
                    "gs://dataplex-clouddq-api-integration-test/profiles.yml",
                ],
            },
            "execution_spec": {
                "args": {
                    "TASK_ARGS": f'{"clouddq_patched.zip, ALL, configs,"}'
                    f'{"--dbt_profiles_dir=.,"}'
                    f'{"--environment_target=bq, --skip_sql_validation"}'
                }
            },
        }

        default_body.update(body)

        response = DataplexClient.createDataplexTask(
            self,
            dataplex_endpoint=self.dataplex_endpoint,
            project_id=self.project_id,
            location_id=self.location_id,
            lake_name=self.lake_name,
            task_id=task_id,
            session=self.session,
            headers=self.headers,
            body=default_body,
        )

        return response

    def getCloudDQTaskJobs(self, task_id: str) -> Response:

        """
        List the dataplex task jobs
        :param task_id: task id for dataplex task
        :return: Response object
        """

        response = DataplexClient.getDataplexTaskJobs(
            self,
            dataplex_endpoint=self.dataplex_endpoint,
            project_id=self.project_id,
            location_id=self.location_id,
            lake_name=self.lake_name,
            task_id=task_id,
            session=self.session,
            headers=self.headers,
        )

        return response

    def getCloudDQTaskStatus(self, task_id: str) -> str:

        """
        Get the dataplex task status
        :param task_id: dataplex task id
        :return: Task status
        """
        try:
            res = self.getCloudDQTaskJobs(task_id)
            logger.debug(res.status_code)
            logger.debug(res.text)
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
                    self.getCloudDQTaskStatus(task_id)

        except RuntimeError:
            logger.debug(traceback.format_exc())
