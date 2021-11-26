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

from pathlib import Path

import json
import logging

from google.auth.credentials import Credentials
from requests import Response
from requests import Session
from requests_oauth2 import OAuth2BearerToken
from pprint import pformat

import google.auth
import google.auth.transport.requests

from clouddq.integration.gcp_credentials import GcpCredentials


logger = logging.getLogger(__name__)


class DataplexClient:
    _gcp_credentials: GcpCredentials
    _headers: dict
    _session: Session
    _auth_token: str
    gcp_project_id: str
    location_id: str
    lake_name: str
    dataplex_endpoint: str

    def __init__(
        self,
        gcp_project_id: str | None = None,
        gcp_dataplex_region: str | None = None,
        gcp_dataplex_lake_name: str | None = None,
        gcp_credentials: GcpCredentials | None = None,
        gcp_service_account_key_path: Path | None = None,
        gcp_impersonation_credentials: str | None = None,
        dataplex_endpoint: str = "https://dataplex.googleapis.com",
    ) -> None:
        if gcp_credentials:
            self._gcp_credentials = gcp_credentials
        else:
            self._gcp_credentials = GcpCredentials(
                gcp_project_id=gcp_project_id,
                gcp_service_account_key_path=gcp_service_account_key_path,
                gcp_impersonation_credentials=gcp_impersonation_credentials,
            )
        self._auth_token = self._get_auth_token(
            credentials=self._gcp_credentials.credentials
        )
        self._headers = self._set_headers()
        self._session = self._get_session()
        self.gcp_project_id = gcp_project_id
        self.lake_name = gcp_dataplex_lake_name
        self.location_id = gcp_dataplex_region
        self.dataplex_endpoint = dataplex_endpoint
        assert self.dataplex_endpoint is not None

    def _get_auth_token(self, credentials: Credentials) -> str:
        """
        This method is used to get the authentication token.

        Returns:
        auth_token (str):
        """
        # getting request object
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)  # refresh token
        # check for valid credentials
        auth_token = credentials.token
        return auth_token

    def _set_headers(self) -> dict:
        # create request headers
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self._auth_token,
        }

        return headers

    def _get_session(self) -> Session:
        """
        This method create the session object for request
        :return:
        session object
        """
        with Session() as session:
            session.auth = OAuth2BearerToken(self._auth_token)
        return session

    def get_dataplex_lake(
        self,
        lake_name: str,
        gcp_project_id: str = None,
        location_id: str = None,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not location_id or not gcp_project_id:
            raise ValueError(
                "Dataplex Client API call missing required arguments 'gcp_project_id' and 'location_id'."
            )
        response = self._session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}",
            headers=self._headers,
        )
        return response

    def create_dataplex_task(
        self,
        task_id: str,
        post_body: dict,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
        validate_only: bool = False,
    ) -> Response:
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        if not location_id or not gcp_project_id or not lake_name:
            raise ValueError(
                "Dataplex Client API call missing required arguments 'gcp_project_id', 'lake_name' and 'location_id'."
            )
        request_url = (
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks"
        )
        params = {"task_id": task_id}
        if validate_only:
            params["validate_only"] = True
        response = self._session.post(
            request_url,
            headers=self._headers,
            params=params,
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
        if not location_id or not gcp_project_id or not lake_name:
            raise ValueError(
                "Dataplex Client API call missing required arguments 'gcp_project_id', 'lake_name' and 'location_id'."
            )
        response = self._session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}/jobs",
            headers=self._headers,
        )
        return response

    def get_dataplex_task(
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
        if not location_id or not gcp_project_id or not lake_name:
            raise ValueError(
                "Dataplex Client API call missing required arguments 'gcp_project_id', 'lake_name' and 'location_id'."
            )
        response = self._session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}",
            headers=self._headers,
        )
        return response

    def delete_dataplex_task(
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
        if not location_id or not gcp_project_id or not lake_name:
            raise ValueError(
                "Dataplex Client API call missing required arguments 'gcp_project_id', 'lake_name' and 'location_id'."
            )
        response = self._session.delete(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}",
            headers=self._headers,
        )
        return response

    def get_dataplex_iam_permissions(
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
        if not location_id or not gcp_project_id or not lake_name:
            raise ValueError(
                "Dataplex Client API call missing required arguments 'gcp_project_id', 'lake_name' and 'location_id'."
            )
        response = self._session.post(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/",
            headers=self._headers,
            data=json.dumps(body),
        )
        return response

    def get_entity(
        self,
        zone_id: str,
        entity_id: str,
        params: dict = None,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        logger.debug(f"DataplexClient.get_entity() arguments: {locals()}")
        if not zone_id:
            raise ValueError("zone_id is a required argument.")
        if not entity_id:
            raise ValueError("entity_id is a required argument.")
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        if not location_id:
            raise ValueError(
                "Dataplex get_entity() API call missing required arguments 'location_id'."
            )
        if not gcp_project_id:
            raise ValueError(
                "Dataplex get_entity() API call missing required arguments 'gcp_project_id'."
            )
        if not lake_name:
            raise ValueError(
                "Dataplex get_entity() API call missing required arguments 'lake_name'."
            )
        response = self._session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/zones/{zone_id}/entities/{entity_id}",
            headers=self._headers,
            params=params,
        )
        return response

    def list_entities(
        self,
        zone_id: str,
        params: dict = None,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> Response:
        logger.debug(f"DataplexClient.list_entities() arguments: {locals()}")
        if not zone_id:
            raise ValueError("zone_id is a required argument.")
        if not gcp_project_id:
            gcp_project_id = self.gcp_project_id
        if not location_id:
            location_id = self.location_id
        if not lake_name:
            lake_name = self.lake_name
        if not location_id:
            raise ValueError(
                "Dataplex list_entities() API call missing required arguments 'location_id'."
            )
        if not gcp_project_id:
            raise ValueError(
                "Dataplex list_entities() API call missing required arguments 'gcp_project_id'."
            )
        if not lake_name:
            raise ValueError(
                "Dataplex list_entities() API call missing required arguments 'lake_name'."
            )
        response = self._session.get(
            f"{self.dataplex_endpoint}/v1/projects/{gcp_project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/zones/{zone_id}/entities/",
            headers=self._headers,
            params=params,
        )
        if "entities" not in response.json():
            logger.warning(
                f"\nFailed to retrieve entities matching filter:\n"
                f" '{params['filter']}'\n"
                f"in Dataplex zone:\n"
                f" '/projects/{gcp_project_id}/locations/{location_id}"
                f"/lakes/{lake_name}/zones/{zone_id}'.\n\n"
                f"Arguments used in list_entities API call:\n"
                f"{gcp_project_id=}\n"
                f"{location_id=}\n"
                f"{lake_name=}\n"
                f"{zone_id=}\n" 
                f"{params=}\n\n"
                f"API response:\n"
                f"Response status code: {pformat(response.status_code)}\n"
                f"Response body: {pformat(response.json())}\n\n"
                "Ensure 1) you have sufficient IAM permission to query the Dataplex zone "
                "and 2) the table is registered in the correct Dataplex zone.\n"
            )
        return response
