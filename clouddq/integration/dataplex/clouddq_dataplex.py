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
import re

from requests import Response

from clouddq.integration.dataplex.dataplex_client import DataplexClient
from clouddq.integration.gcp_credentials import GcpCredentials
from clouddq.integration.gcs import upload_blob


logger = logging.getLogger(__name__)
TARGET_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]
USER_AGENT_TAG = "Product_Dataplex/1.0 (GPN:Dataplex_CloudDQ)"
DEFAULT_GCS_BUCKET_NAME = "dataplex-clouddq-api-integration"


class CloudDqDataplexClient:
    __client: DataplexClient
    gcs_bucket_name: str

    def __init__(
        self,
        gcp_credentials: GcpCredentials,
        gcp_project_id: str,
        gcp_dataplex_lake_name: str,
        gcp_dataplex_region: str,
        gcs_bucket_name: str,
        dataplex_endpoint: str = "https://dataplex.googleapis.com",
    ) -> None:
        if gcs_bucket_name:
            self.gcs_bucket_name = gcs_bucket_name
        else:
            # self.gcs_bucket_name = f"{DEFAULT_GCS_BUCKET_NAME}_{location_id}"
            self.gcs_bucket_name = DEFAULT_GCS_BUCKET_NAME
        self.__client = DataplexClient(
            gcp_credentials=gcp_credentials,
            gcp_project_id=gcp_project_id,
            gcp_dataplex_lake_name=gcp_dataplex_lake_name,
            gcp_dataplex_region=gcp_dataplex_region,
            dataplex_endpoint=dataplex_endpoint,
        )

    def create_clouddq_task(
        self,
        task_id: str,
        trigger_spec_type: str,
        task_description: str,
        data_quality_spec_file_path: str,
        result_dataset_name: str,
        result_table_name: str,
        service_account: str,
        labels: dict | None = None,
        clouddq_pyspark_driver_path: str | None = None,
    ) -> Response:

        if not clouddq_pyspark_driver_path:
            python_script_file = (
                f"gs://{self.gcs_bucket_name}/clouddq_pyspark_driver.py"
            )
        else:
            python_script_file = clouddq_pyspark_driver_path

        if data_quality_spec_file_path[:5] == "gs://":
            data_quality_spec_file_path = data_quality_spec_file_path
        else:
            upload_blob(
                self.gcs_bucket_name,
                data_quality_spec_file_path,
                data_quality_spec_file_path,
            )
            gcs_uri = f"gs://{self.gcs_bucket_name}/{data_quality_spec_file_path}"
            data_quality_spec_file_path = gcs_uri

        allowed_user_agent_label = re.sub("[^0-9a-zA-Z]+", "-", USER_AGENT_TAG.lower())
        if labels:
            labels["user-agent"] = allowed_user_agent_label
        else:
            labels = {"user-agent": allowed_user_agent_label}

        clouddq_post_body = {
            "spark": {
                "python_script_file": python_script_file,
                "file_uris": [
                    f"gs://{self.gcs_bucket_name}/clouddq-executable.zip",
                    f"gs://{self.gcs_bucket_name}/clouddq-executable.zip.hashsum",
                    f"{data_quality_spec_file_path}",
                ],
            },
            "execution_spec": {
                "args": {
                    "TASK_ARGS": f"clouddq-executable.zip, "
                    "ALL, "
                    f"{data_quality_spec_file_path}, "
                    f'--gcp_project_id="{self.gcp_project_id}", '
                    f'--gcp_region_id="{self.gcp_bq_region}", '
                    f'--gcp_bq_dataset_id="{self.gcp_bq_dataset}", '
                    f"--target_bigquery_summary_table="
                    f'"{self.gcp_project_id}.'
                    f"{result_dataset_name}."
                    f'{result_table_name}",'
                },
                "service_account": f"{service_account}",
            },
            "trigger_spec": {"type": trigger_spec_type},
            "description": task_description,
            "labels": labels,
        }

        response = self.__client.create_dataplex_task(
            task_id=task_id,
            post_body=clouddq_post_body,
        )

        return response

    def get_clouddq_task_status(self, task_id: str) -> str:

        """
        Get the dataplex task status
        :param task_id: dataplex task id
        :return: Task status
        """
        res = self.__client.get_clouddq_task_jobs(task_id)
        logger.debug(f"Response status code is {res.status_code}")
        logger.debug(f"Response text is {res.text}")
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

        get_task_response = self.__client.get_task(
            task_id=task_id,
        )
        if get_task_response.status_code == 200:
            delete_task_response = self.__client.delete_task(
                task_id=task_id,
            )
            return delete_task_response
        else:
            return get_task_response

    def get_iam_permissions(self) -> list:

        body = {"resource": "dataplex", "permissions": ["roles/dataproc.worker"]}
        return self.__client.get_iam_permissions(
            body=body,
        )
