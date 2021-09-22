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
from requests import Session, Response
import google.auth
from requests_oauth2 import OAuth2BearerToken
import google.auth.transport.requests
import time

# getting the credentials and project details for gcp project
credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])


def getAuthToken(credentials):

    """
    This method is used to get the authentication token.

    Returns:
    auth_token (str):
    """

    # getting request object
    auth_req = google.auth.transport.requests.Request()

    print(credentials.valid)  # logger.debugs False
    credentials.refresh(auth_req)  # refresh token
    # check for valid credentials
    print(credentials.valid)  # logger.debugs True
    auth_token = credentials.token
    print(auth_token)

    return auth_token

# get auth token
auth_token = getAuthToken(credentials)

# create request headers
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + auth_token
}

def getSession():
    """
    This method create the session object for request
    :return:
    session object
    """
    with Session() as session:
        session.auth = OAuth2BearerToken(auth_token)

    return session


session = getSession()


class DqDataplex():

    dataplex_endpoint: str
    project_id: str
    location_id: str
    lake_name: str

    def __init__(self, dataplex_endpoint, project_id, location_id, lake_name):
        self.dataplex_endpoint = dataplex_endpoint
        self.project_id = project_id
        self.location_id = location_id
        self.lake_name = lake_name

    def createDataplexTask(self, task_id: str, body: dict) -> Response:

        """
        Creates dataplex task
        :param task_id: task id for dataplex task
        :param body: request body for the task
        :return: Response object
        """
        response = session.post(
            f"{self.dataplex_endpoint}/v1/projects/{self.project_id}/locations/{self.location_id}/lakes/{self.lake_name}/tasks?task_id={task_id}",
            headers=headers, data=json.dumps(body))  # create task api

        return response

    def listDataplexTaskJobs(self, task_id: str) -> Response:

        """
        List the dataplex task jobs
        :param task_id: task id for dataplex task
        :return: Response object
        """

        res = session.get(
            f"{self.dataplex_endpoint}/v1/projects/{self.project_id}/locations/{self.location_id}/lakes/{self.lake_name}/tasks/{task_id}/jobs",
            headers=headers)
        return res

    def getDataplexTaskStatus(self, task_id: str) -> str:

        """
        Get the dataplex task status
        :param task_id: dataplex task id
        :return: Task status
        """

        res = self.listDataplexTaskJobs(task_id)
        print(res.status_code)
        print(res.text)
        resp_obj = json.loads(res.text)
        if res.status_code == 200 and len(resp_obj['jobs']) > 0:
            if 'state' in resp_obj['jobs'][0]:
                task_status = resp_obj['jobs'][0]['state']
                return task_status
            else:
                time.sleep(60)
                self.getDataplexTaskStatus(task_id)
