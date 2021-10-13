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

from requests import Response
from requests import Session


class DataplexClient:

    def __init__(self):
        pass

    def create_dataplex_task(
            self,
            dataplex_endpoint: str,
            project_id: str,
            location_id: str,
            lake_name: str,
            task_id: str,
            session: Session,
            headers: dict,
            body: dict,
    ) -> Response:
        """
        :param dataplex_endpoint:
        :param project_id:
        :param location_id:
        :param lake_name:
        :param task_id:
        :param session:
        :param headers:
        :param body:
        :return: Response object
        """

        response = session.post(
            f"{dataplex_endpoint}/v1/projects/{project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks?task_id={task_id}",
            headers=headers,
            data=json.dumps(body),
        )  # create task api

        return response

    def get_dataplex_task_jobs(
            self,
            dataplex_endpoint: str,
            project_id: str,
            location_id: str,
            lake_name: str,
            task_id: str,
            session: Session,
            headers: dict,
    ) -> Response:
        """
        :param dataplex_endpoint:
        :param project_id:
        :param location_id:
        :param lake_name:
        :param task_id:
        :param session:
        :param headers:
        :return:
        """

        response = session.get(
            f"{dataplex_endpoint}/v1/projects/{project_id}/locations/"
            f"{location_id}/lakes/{lake_name}/tasks/{task_id}/jobs",
            headers=headers,
        )

        return response
