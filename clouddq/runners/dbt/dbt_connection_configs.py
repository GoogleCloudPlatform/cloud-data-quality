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

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from enum import auto
from enum import unique
from pathlib import Path
from typing import Dict
from typing import Optional

import yaml


logger = logging.getLogger(__name__)

DEFAULT_DBT_ENVIRONMENT_TARGET = "dev"
DBT_PROFILES_YML_TEMPLATE = {
    "default": {
        "target": DEFAULT_DBT_ENVIRONMENT_TARGET,
        "outputs": {DEFAULT_DBT_ENVIRONMENT_TARGET: {}},
    }
}


@unique
class DbtBigQueryConnectionMethod(str, Enum):
    """Defines supported connection method to BigQuery via dbt-bigquery"""

    OAUTH = auto()
    SERVICE_ACCOUNT_KEY = auto()
    SERVICE_ACCOUNT_IMPERSONATION = auto()


@dataclass
class DbtConnectionConfig(ABC):
    """Abstract base class for dbt connection profiles configurations."""

    @abstractmethod
    def to_dbt_profiles_dict(self) -> Dict:
        pass

    def to_dbt_profiles_yml(
        self,
        target_directory: Optional[Path] = None,
        environment_target: str = DEFAULT_DBT_ENVIRONMENT_TARGET,
    ) -> str:
        template = DBT_PROFILES_YML_TEMPLATE
        profiles_content = self.to_dbt_profiles_dict()
        template["default"]["target"] = environment_target
        template["default"]["outputs"][environment_target] = profiles_content
        if target_directory:
            with open(Path(target_directory).joinpath("profiles.yml"), "w") as f:
                yaml.dump(template, f)
        return yaml.dump(template)


@dataclass
class GcpDbtConnectionConfig(DbtConnectionConfig):
    """Data class for dbt connection profiles configurations to GCP."""

    gcp_project_id: str
    gcp_region_id: str
    gcp_bq_dataset_id: str
    connection_method: DbtBigQueryConnectionMethod
    gcp_service_account_key_path: Optional[str]
    service_account_gcp_impersonation_credentials: Optional[str]
    threads: int = 1
    timeout_seconds: int = 600
    priority: str = "interactive"
    retries: int = 1

    def __init__(
        self,
        gcp_project_id: str,
        gcp_region_id: str,
        gcp_bq_dataset_id: str,
        gcp_service_account_key_path: Optional[str],
        gcp_impersonation_credentials: Optional[str],
    ):
        if not gcp_project_id:
            raise ValueError(
                f"Invalid input to connection config argument 'gcp_project_id': "
                f"{gcp_project_id}."
            )
        if not gcp_region_id:
            raise ValueError(
                f"Invalid input to connection config argument 'gcp_region_id': "
                f"{gcp_region_id}."
            )
        if not gcp_bq_dataset_id:
            raise ValueError(
                f"Invalid input to connection config argument 'gcp_bq_dataset_id': "
                f"{gcp_bq_dataset_id}."
            )
        self.gcp_project_id = gcp_project_id
        self.gcp_region_id = gcp_region_id
        self.gcp_bq_dataset_id = gcp_bq_dataset_id
        defined_connection_types = [
            conn
            for conn in [gcp_service_account_key_path, gcp_impersonation_credentials]
            if conn
        ]
        if not any(defined_connection_types):
            logger.info(
                "Using Application-Default Credentials (ADC) to connect to GCP..."
            )
            self.connection_method = DbtBigQueryConnectionMethod.OAUTH
        elif all(defined_connection_types):
            raise ValueError(
                "Either one or neither but not both of service account JSON key "
                "or service account impersonation can be used."
            )
        elif gcp_service_account_key_path:
            logger.info("Using exported service account key to connect to GCP...")
            self.gcp_service_account_key_path = gcp_service_account_key_path
            self.connection_method = DbtBigQueryConnectionMethod.SERVICE_ACCOUNT_KEY
        elif gcp_impersonation_credentials:
            logger.info(
                "Using service account impersonation via local ADC credentials "
                "to connect to GCP..."
            )
            self.service_account_gcp_impersonation_credentials = (
                gcp_impersonation_credentials
            )
            self.connection_method = (
                DbtBigQueryConnectionMethod.SERVICE_ACCOUNT_IMPERSONATION
            )
        else:
            raise ValueError("Unable to create dbt connection profile for GCP.")

    def get_connection_method(self) -> str:
        if (
            self.connection_method == DbtBigQueryConnectionMethod.OAUTH
            or self.connection_method  # noqa: W503
            == DbtBigQueryConnectionMethod.SERVICE_ACCOUNT_IMPERSONATION  # noqa: W503
        ):
            return "oauth"
        elif self.connection_method.SERVICE_ACCOUNT_KEY:
            return "service-account"
        else:
            raise ValueError("Unable to get dbt connection method for GCP.")

    def to_dbt_profiles_dict(self) -> Dict:
        profiles_configs = {
            "type": "bigquery",
            "method": self.get_connection_method(),
            "project": self.gcp_project_id,
            "dataset": self.gcp_bq_dataset_id,
            "location": self.gcp_region_id,
            "threads": self.threads,
            "timeout_seconds": self.timeout_seconds,
            "priority": self.priority,
            "retries": self.retries,
        }
        if self.connection_method == DbtBigQueryConnectionMethod.SERVICE_ACCOUNT_KEY:
            assert Path(self.gcp_service_account_key_path).is_file()
            profiles_configs["keyfile"] = self.gcp_service_account_key_path
        elif (
            self.connection_method
            == DbtBigQueryConnectionMethod.SERVICE_ACCOUNT_IMPERSONATION  # noqa: W503
        ):
            assert self.service_account_gcp_impersonation_credentials
            assert profiles_configs["method"] == "oauth"
            profiles_configs[
                "impersonate_service_account"
            ] = self.service_account_gcp_impersonation_credentials
        else:
            assert profiles_configs["method"] == "oauth"
        return profiles_configs
