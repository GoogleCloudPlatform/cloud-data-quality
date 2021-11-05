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

import json
import logging

from pathlib import Path

import google.auth
import google.auth.transport.requests

from google.auth.credentials import Credentials
from requests import Response
from requests import Session
from requests_oauth2 import OAuth2BearerToken

from clouddq.integration.gcp_credentials import GcpCredentials


logger = logging.getLogger(__name__)


class DataplexClient:
    __gcp_credentials: GcpCredentials
    __headers: dict
    __session: Session
    __auth_token: str
    gcp_project_id: str
    location_id: str
    lake_name: str
    dataplex_endpoint: str

    def __init__(
        self,
        gcp_credentials: GcpCredentials,
        gcp_dataplex_lake_name: str,
        gcp_dataplex_region: str,
        gcp_project_id: str,
        gcp_service_account_key_path: Path = None,
        gcp_impersonation_credentials: str = None,
        dataplex_endpoint: str = "https://dataplex.googleapis.com",
    ) -> None:
        if gcp_credentials:
            self._gcp_credentials = gcp_credentials
        else:
            self._gcp_credentials = GcpCredentials(
                project_id=gcp_project_id,
                gcp_service_account_key_path=gcp_service_account_key_path,
                gcp_impersonation_credentials=gcp_impersonation_credentials,
            )
        self.__auth_token = self.__get_auth_token(
            credentials=self.__gcp_credentials.credentials
        )
        self.__headers = self.__set_headers()
        self.__session = self.__get_session()
        self.gcp_project_id = gcp_project_id
        self.dataplex_endpoint = dataplex_endpoint
        self.lake_name = gcp_dataplex_lake_name
        self.location_id = gcp_dataplex_region

    def __get_auth_token(self, credentials: Credentials) -> str:
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

    def __set_headers(self) -> dict:
        # create request headers
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.__auth_token,
        }

        return headers

    def __get_session(self) -> Session:
        """
        This method create the session object for request
        :return:
        session object
        """
        with Session() as session:
            session.auth = OAuth2BearerToken(self.__auth_token)
        return session

    def create_dataplex_task(
        self,
        task_id: str,
        post_body: dict,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        response = self.__session.post(
            f"{self.dataplex_endpoint}/v1/projects/{self.gcp_project_id}/locations/"
            f"{self.location_id}/lakes/{self.lake_name}/tasks?task_id={task_id}",
            headers=self.__headers,
            data=json.dumps(post_body),
        )
        return response

    def get_dataplex_task_jobs(
        self,
        task_id: str,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        response = self.__session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}/jobs",
            headers=self.__headers,
        )
        return response

    def get_task(
        self,
        task_id: str,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        response = self.__session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}",
            headers=self.__headers,
        )

        return response

    def delete_task(
        self,
        task_id: str,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        response = self.__session.delete(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}",
            headers=self.__headers,
        )

        return response

    def get_iam_permissions(
        self,
        body: str,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        response = self.__session.post(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/",
            headers=self.__headers,
            data=json.dumps(body),
        )
        return response
