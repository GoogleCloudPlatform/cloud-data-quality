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

from enum import Enum
from pathlib import Path
from pprint import pformat

import json
import logging
import re
import time
import typing

from requests import Response

from clouddq.classes.dataplex_entity import DataplexEntity
from clouddq.integration.dataplex.dataplex_client import DataplexClient
from clouddq.integration.gcp_credentials import GcpCredentials
from clouddq.integration.gcs import upload_blob
from clouddq.utils import exponential_backoff
from clouddq.utils import update_dict


logger = logging.getLogger(__name__)
TARGET_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]
USER_AGENT_TAG = "Product_Dataplex/1.0 (GPN:Dataplex_CloudDQ)"
DEFAULT_GCS_BUCKET_NAME = "dataplex-clouddq-artifacts-{gcp_dataplex_region}"


class DATAPLEX_TASK_TRIGGER_TYPE(str, Enum):
    ON_DEMAND = "ON_DEMAND"
    RECURRING = "RECURRING"


class CloudDqDataplexClient:
    _client: DataplexClient
    gcs_bucket_name: str

    def __init__(
        self,
        gcp_project_id: str | None = None,
        gcp_dataplex_lake_name: str | None = None,
        gcp_dataplex_region: str | None = None,
        gcs_bucket_name: str | None = None,
        gcp_credentials: GcpCredentials | None = None,
        dataplex_endpoint: str = "https://dataplex.googleapis.com",
    ) -> None:
        if gcs_bucket_name:
            self.gcs_bucket_name = gcs_bucket_name
        else:
            self.gcs_bucket_name = DEFAULT_GCS_BUCKET_NAME.format(
                gcp_dataplex_region=gcp_dataplex_region
            )
        self._client = DataplexClient(
            gcp_credentials=gcp_credentials,
            gcp_project_id=gcp_project_id,
            gcp_dataplex_lake_name=gcp_dataplex_lake_name,
            gcp_dataplex_region=gcp_dataplex_region,
            dataplex_endpoint=dataplex_endpoint,
        )

    def create_clouddq_task(  # noqa: C901
        self,
        task_id: str,
        clouddq_yaml_spec_file_path: str,
        clouddq_run_project_id: str,
        clouddq_run_bq_region: str,
        clouddq_run_bq_dataset: str,
        task_service_account: str,
        target_bq_result_project_name: str,
        target_bq_result_dataset_name: str,
        target_bq_result_table_name: str,
        task_trigger_spec_type: DATAPLEX_TASK_TRIGGER_TYPE = DATAPLEX_TASK_TRIGGER_TYPE.ON_DEMAND,  # noqa: E501
        task_description: str | None = None,
        task_labels: dict | None = None,
        clouddq_pyspark_driver_path: str | None = None,
        clouddq_executable_path: str | None = None,
        clouddq_executable_checksum_path: str | None = None,
        validate_only: bool = False,
        clouddq_pyspark_driver_filename: str = "clouddq_pyspark_driver.py",
    ) -> Response:
        # Set default CloudDQ PySpark driver path if not manually overridden
        clouddq_pyspark_driver_path = self._validate_clouddq_artifact_path(
            clouddq_pyspark_driver_path, clouddq_pyspark_driver_filename
        )
        # Set default CloudDQ executable path if not manually overridden
        clouddq_executable_path = self._validate_clouddq_artifact_path(
            clouddq_executable_path, "clouddq-executable.zip"
        )
        # Set default CloudDQ executable checksum path if not manually overridden
        clouddq_executable_checksum_path = self._validate_clouddq_artifact_path(
            clouddq_executable_checksum_path, "clouddq-executable.zip.hashsum"
        )
        # Prepare input CloudDQ YAML specs path
        clouddq_yaml_spec_file_path = str(clouddq_yaml_spec_file_path)
        if clouddq_yaml_spec_file_path[:5] == "gs://":
            clouddq_configs_gcs_path = clouddq_yaml_spec_file_path
        else:
            clouddq_yaml_spec_file_path = Path(clouddq_yaml_spec_file_path)
            if clouddq_yaml_spec_file_path.is_file():
                upload_blob(
                    self.gcs_bucket_name,
                    clouddq_yaml_spec_file_path.name,
                    str(clouddq_yaml_spec_file_path.name),
                )
                gcs_uri = (
                    f"gs://{self.gcs_bucket_name}/{clouddq_yaml_spec_file_path.name}"
                )
                clouddq_configs_gcs_path = gcs_uri
            else:
                raise ValueError(
                    "'clouddq_yaml_spec_file_path' argument "
                    f"{clouddq_yaml_spec_file_path} "
                    "must either be a single file (`.yml` or `.zip`) "
                    "or a GCS path to the `.yml` or `.zip` configs file."
                )
        # Add user-agent tag as Task label
        allowed_user_agent_label = re.sub("[^0-9a-zA-Z]+", "-", USER_AGENT_TAG.lower())
        if task_labels:
            task_labels["user-agent"] = allowed_user_agent_label
        else:
            task_labels = {"user-agent": allowed_user_agent_label}
        # Prepre Dataplex Task message body for CloudDQ Job
        clouddq_post_body = {
            "spark": {
                "python_script_file": clouddq_pyspark_driver_path,
                "file_uris": [
                    f"{clouddq_executable_path}",
                    f"{clouddq_executable_checksum_path}",
                    f"{clouddq_configs_gcs_path}",
                ],
            },
            "execution_spec": {
                "args": {
                    "TASK_ARGS": f"clouddq-executable.zip, "
                    "ALL, "
                    f"{clouddq_configs_gcs_path}, "
                    f'--gcp_project_id="{clouddq_run_project_id}", '
                    f'--gcp_region_id="{clouddq_run_bq_region}", '
                    f'--gcp_bq_dataset_id="{clouddq_run_bq_dataset}", '
                    f"--target_bigquery_summary_table="
                    f'"{target_bq_result_project_name}.'
                    f"{target_bq_result_dataset_name}."
                    f'{target_bq_result_table_name}",'
                },
                "service_account": f"{task_service_account}",
            },
            "trigger_spec": {"type": task_trigger_spec_type},
            "description": task_description,
            "labels": task_labels,
        }
        # Set trigger_spec for RECURRING trigger type
        if task_trigger_spec_type == DATAPLEX_TASK_TRIGGER_TYPE.RECURRING:
            raise NotImplementedError(
                f"task_trigger_spec_type {task_trigger_spec_type} not yet supported."
            )
        response = self._client.create_dataplex_task(
            task_id=task_id,
            post_body=clouddq_post_body,
            validate_only=validate_only,
        )
        return response

    def get_clouddq_task_status(self, task_id: str) -> str:

        """
        Get the dataplex task status
        :param task_id: dataplex task id
        :return: Task status
        """
        res = self._client.get_dataplex_task_jobs(task_id)
        logger.info(f"Response status code is {res.status_code}")
        logger.info(f"Response text is {res.text}")
        resp_obj = json.loads(res.text)

        if res.status_code == 200:

            if (
                "jobs" in resp_obj
                and len(resp_obj["jobs"]) > 0  # noqa: W503
                and "state" in resp_obj["jobs"][0]  # noqa: W503
            ):
                task_status = resp_obj["jobs"][0]["state"]
                return task_status
        else:
            return res

    def delete_clouddq_task_if_exists(self, task_id: str) -> Response:

        """
        List the dataplex task jobs
        :param task_id: task id for dataplex task
        :return: Response object
        """
        get_task_response = self._client.get_dataplex_task(
            task_id=task_id,
        )
        if get_task_response.status_code == 200:
            delete_task_response = self._client.delete_dataplex_task(
                task_id=task_id,
            )
            return delete_task_response
        else:
            return get_task_response

    def get_dataplex_lake(self, lake_name: str) -> Response:
        return self._client.get_dataplex_lake(lake_name)

    def get_iam_permissions(self) -> list:
        body = {"resource": "dataplex", "permissions": ["roles/dataproc.worker"]}
        return self._client.get_dataplex_iam_permissions(
            body=body,
        )

    def _validate_clouddq_artifact_path(
        self, clouddq_artifact_path: str | None, artifact_name: str
    ) -> str:
        if not clouddq_artifact_path:
            clouddq_artifact_gcs_path = f"gs://{self.gcs_bucket_name}/{artifact_name}"
        else:
            clouddq_artifact_path = str(clouddq_artifact_path)
            clouddq_artifact_name = clouddq_artifact_path.split("/")[-1]
            if not clouddq_artifact_path[:5] == "gs://":
                raise ValueError(
                    f"Artifact path argument for {artifact_name}: "
                    f"{clouddq_artifact_path} must be a GCS path."
                )
            elif clouddq_artifact_name != artifact_name:
                raise ValueError(
                    f"Artifact path argument for {artifact_name}: "
                    f"{clouddq_artifact_path} must end with '{artifact_name}'."
                )
            else:
                clouddq_artifact_gcs_path = clouddq_artifact_path
        return clouddq_artifact_gcs_path

    def get_dataplex_entity(
        self,
        zone_id: str,
        entity_id: str,
        gcp_project_id: str = None,
        location_id: str = None,
        lake_name: str = None,
    ) -> DataplexEntity:
        params = {"view": "FULL"}
        response = self._client.get_entity(
            zone_id=zone_id,
            entity_id=entity_id,
            gcp_project_id=gcp_project_id,
            location_id=location_id,
            lake_name=lake_name,
            params=params,
        )
        if response.status_code == 200:
            return DataplexEntity.from_dict(json.loads(response.text))
        else:
            raise RuntimeError(
                f"Failed to retrieve Dataplex entity: '/projects/{self._client.gcp_project_id}/locations/{self._client.location_id}/lakes/{self._client.lake_name}/zones/{zone_id}/entities/{entity_id}':\n {response.text}"
            )

    def list_dataplex_entities(
            self,
            zone_id: str,
            prefix: str = None,
            data_path: str = None,
            gcp_project_id: str = None,
            location_id: str = None,
            lake_name: str = None,
    ) -> typing.List[DataplexEntity]:
        params = {"page_size": 1000}
        logger.info(f"Prefix value for filter is {prefix}")

        if prefix and data_path:
            raise ValueError(f"Either prefix or datapath should be passed not both")
        if prefix:
            params.update({"filter": f"id=starts_with({prefix})"})
        if data_path:
            params.update({"filter": f"data_path=starts_with({data_path})"})

        logger.info(f"Initial params are {params}")
        response = self._client.list_entities(
            zone_id=zone_id,
            params=params,
            gcp_project_id=gcp_project_id,
            location_id=location_id,
            lake_name=lake_name,
        ).json()

        print("params", params)

        while "nextPageToken" in response:
            time.sleep(3)  # to avoid api limit exceed error of 4 calls per 10 sec
            next_page_token = response["nextPageToken"]
            logger.info(f"Next Page Token {next_page_token}")
            page_token = {"page_token": f"{next_page_token}"}
            params.update(page_token)
            logger.info(f"Updated Params are {params}")
            next_page_response = self._client.list_entities(
                zone_id, params=params
            ).json()
            logger.info(f"Next page response {next_page_response}")

            if "nextPageToken" not in next_page_response:
                del response["nextPageToken"]
                response = update_dict(response, next_page_response)
            else:
                response = update_dict(response, next_page_response)
                response["nextPageToken"] = next_page_response["nextPageToken"]

        if "entities" not in response:
            raise RuntimeError(
                f"Failed to list Dataplex zone '/projects/{self._client.gcp_project_id}/locations/{self._client.location_id}/lakes/{self._client.lake_name}/zones/{zone_id}':\n {pformat(response)}"
            )

        dataplex_entities = []
        for entity in response["entities"]:
            entity_with_schema = self.get_dataplex_entity(
                zone_id=zone_id, entity_id=entity["id"]
            )
            dataplex_entities.append(entity_with_schema)
        return dataplex_entities
