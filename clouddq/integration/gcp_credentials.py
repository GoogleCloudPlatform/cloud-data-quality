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

from pathlib import Path

import google.auth
import google.auth.transport.requests

from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.auth.exceptions import RefreshError
from google.oauth2 import id_token
from google.oauth2 import service_account


logger = logging.getLogger(__name__)

TARGET_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]


class GcpCredentials:
    credentials: Credentials = None
    project_id: str = None
    user_id: str = None

    def __init__(
        self,
        credentials: Credentials = None,
        gcp_project_id: str = None,
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
            self.credentials = target_credentials
        else:
            # Otherwise use source_credentials
            self.credentials = source_credentials
        self.project_id = self.__resolve_project_id(
            credentials=self.credentials, project_id=gcp_project_id
        )
        self.user_id = self.__resolve_credentials_username(credentials=self.credentials)
        if self.user_id:
            logger.info("Successfully created GCP Client.")
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
            logger.error(
                "Could not get refreshed credentials for GCP. Reauthentication Required."
            )
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
