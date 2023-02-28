# Copyright 2022 Google LLC
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

import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateTaskOperator,
    DataplexDeleteTaskOperator,
)

import google.auth

import json
import requests
import time

DATAPLEX_PROJECT_ID = "your-dataplex-project-id"  # The Google Cloud Project where the Dataplex task will be created
DATAPLEX_REGION = "your-dataplex-region-id"  # The region of the Dataplex Lake where the data quality task will be created.
DATAPLEX_LAKE_ID = "your-dataplex-lake-id"  # dataplex lake id
SERVICE_ACC = "service-account-to-execute-task"  # The service account used for executing the task. Ensure this service account has sufficient IAM permissions on your project including BigQuery Data Editor, BigQuery Job User, Dataplex Editor, Dataproc Worker, Service Usage Consumer
PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME = "your-public-bucket-with-clouddq-executable-and-hashsum" # Public Cloud Storage bucket containing the prebuilt data quality executable artifact and hashsum. There is one bucket per GCP region.
SPARK_FILE_FULL_PATH = f"gs://{PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{DATAPLEX_REGION}/clouddq_pyspark_driver.py"
# Public Cloud Storage bucket containing the driver code for executing data quality job. There is one bucket per GCP region.
CLOUDDQ_EXECUTABLE_FILE_PATH = f"gs://{PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{DATAPLEX_REGION}/clouddq-executable.zip" # The Cloud Storage path containing the prebuilt data quality executable artifact. There is one bucket per GCP region.
CLOUDDQ_EXECUTABLE_HASHSUM_FILE_PATH = f"gs://{PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{DATAPLEX_REGION}/clouddq-executable.zip.hashsum" # The Cloud Storage path containing the prebuilt data quality executable artifact hashsum. There is one bucket per GCP region.
CONFIGS_BUCKET_NAME = "your-gcs-configs-bucket" # The gcs bucket to store data quality YAML configurations input to the data quality task. You can have a single yaml file in .yml or yaml format or a .zip archive containing multiple YAML files.
CONFIGS_PATH = f"gs://{CONFIGS_BUCKET_NAME}/clouddq-configs-updated.zip"  # The Cloud Storage path to your data quality YAML configurations input to the data quality task. You can have a single yaml file in .yml or yaml format or a .zip archive containing multiple YAML files.
DATAPLEX_TASK_ID = "your-dq-dataplex-task-id"  # The unique identifier for the task
TRIGGER_SPEC_TYPE = "ON_DEMAND"  # Trigger type for the job
DATAPLEX_ENDPOINT = 'https://dataplex.googleapis.com' # dataplex endpoint
GCP_PROJECT_ID = "your-gcp-project-id"  # The Google Cloud Project where the BQ jobs will be created
GCP_BQ_DATASET_ID = "your-gcp-bq-dataset-id"  # The BigQuery dataset used for storing the intermediate data quality summary results and the BigQuery views associated with each rule binding
TARGET_BQ_TABLE = "your-target-table-name"  # The BigQuery table where the final results of the data quality checks are stored.
GCP_BQ_REGION = "your-gcp-region-id"  # GCP BQ region where the data is stored
FULL_TARGET_TABLE_NAME = f"{GCP_PROJECT_ID}.{GCP_BQ_DATASET_ID}.{TARGET_BQ_TABLE}"  # The BigQuery table where the final results of the data quality checks are stored.

EXAMPLE_TASK_BODY = {
    "spark": {
        "python_script_file": SPARK_FILE_FULL_PATH,
        "file_uris": [CLOUDDQ_EXECUTABLE_FILE_PATH,
                      CLOUDDQ_EXECUTABLE_HASHSUM_FILE_PATH,
                      CONFIGS_PATH
                      ]
    },
    "execution_spec": {
        "service_account": SERVICE_ACC,
        "args": {
            "TASK_ARGS": f"clouddq-executable.zip, \
                 ALL, \
                 {CONFIGS_PATH}, \
                --gcp_project_id={GCP_PROJECT_ID}, \
                --gcp_region_id={GCP_BQ_REGION}, \
                --gcp_bq_dataset_id={GCP_BQ_DATASET_ID}, \
                --target_bigquery_summary_table={FULL_TARGET_TABLE_NAME}"
        }
    },
    "trigger_spec": {
        "type_": TRIGGER_SPEC_TYPE
    },
    "description": "Clouddq Airflow Task"
}

# for best practices
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# default arguments for the dag
default_args = {
    'owner': 'Clouddq Airflow task Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}


def get_session_headers() -> dict:
    """
    This method is to get the session and headers object for authenticating the api requests using credentials.
    Args:
    Returns: dict
    """
    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # getting request object
    auth_req = google.auth.transport.requests.Request()

    credentials.refresh(auth_req)  # refresh token
    auth_token = credentials.token

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + auth_token
    }

    return headers


def get_clouddq_task_status() -> str:
    """
    This method will return the job status for the task.
    Args:
    Returns: str
    """
    headers = get_session_headers()
    res = requests.get(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{DATAPLEX_PROJECT_ID}/locations/{DATAPLEX_REGION}/lakes/{DATAPLEX_LAKE_ID}/tasks/{DATAPLEX_TASK_ID}/jobs",
        headers=headers)
    print(res.status_code)
    print(res.text)
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
        return "FAILED"


def _get_dataplex_job_state() -> str:
    """
    This method will try to get the status of the job till it is in either 'SUCCEEDED' or 'FAILED' state.
    Args:
    Returns: str
    """
    task_status = get_clouddq_task_status()
    while (task_status != 'SUCCEEDED' and task_status != 'FAILED' and task_status != 'CANCELLED'
           and task_status != 'ABORTED'):
        print(time.ctime())
        time.sleep(30)
        task_status = get_clouddq_task_status()
        print(f"CloudDQ task status is {task_status}")
    return task_status

def _get_dataplex_task() -> str:
    """
    This method will return the status for the task.
    Args:
    Returns: str
    """
    headers = get_session_headers()
    res = requests.get(
        f"{DATAPLEX_ENDPOINT}/v1/projects/{DATAPLEX_PROJECT_ID}/locations/{DATAPLEX_REGION}/lakes/{DATAPLEX_LAKE_ID}/tasks/{DATAPLEX_TASK_ID}",
        headers=headers)
    if res.status_code == 200:
        return "task_exist"
    else:
        return "task_not_exist"

with models.DAG(
        'clouddq_airflow_example',
        catchup=False,
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:
    # Print the dag_run id from the Airflow logs
    print("v2")

    start_op = BashOperator(
        task_id="start_task",
        bash_command="echo start",
        dag=dag,
    )

    # this will check for the existing dataplex task 
    get_dataplex_task = BranchPythonOperator(
        task_id="get_dataplex_task",
        python_callable=_get_dataplex_task,
        provide_context=True
    )

    dataplex_task_exists = BashOperator(
        task_id="task_exist",
        bash_command="echo 'Task Already Exists'",
        dag=dag,
    )
    dataplex_task_not_exists = BashOperator(
        task_id="task_not_exist",
        bash_command="echo 'Task not Present'",
        dag=dag,
    )

    # this will delete the existing dataplex task with the given task_id
    delete_dataplex_task = DataplexDeleteTaskOperator(
        project_id=DATAPLEX_PROJECT_ID,
        region=DATAPLEX_REGION,
        lake_id=DATAPLEX_LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="delete_dataplex_task",
    )

    # this will create a new dataplex task with a given task id
    create_dataplex_task = DataplexCreateTaskOperator(
        project_id=DATAPLEX_PROJECT_ID,
        region=DATAPLEX_REGION,
        lake_id=DATAPLEX_LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="create_dataplex_task",
        trigger_rule="none_failed_min_one_success",
    )

    # this will get the status of dataplex task job
    dataplex_task_state = BranchPythonOperator(
        task_id="dataplex_task_state",
        python_callable=_get_dataplex_job_state,
        provide_context=True,

    )

    dataplex_task_success = BashOperator(
        task_id="SUCCEEDED",
        bash_command="echo 'Job Completed Successfully'",
        dag=dag,
    )
    dataplex_task_failed = BashOperator(
        task_id="FAILED",
        bash_command="echo 'Job Failed'",
        dag=dag,
    )

start_op >> get_dataplex_task
get_dataplex_task >> [dataplex_task_exists, dataplex_task_not_exists]
dataplex_task_exists >> delete_dataplex_task
delete_dataplex_task >> create_dataplex_task
dataplex_task_not_exists >> create_dataplex_task
create_dataplex_task >> dataplex_task_state
dataplex_task_state >> [dataplex_task_success, dataplex_task_failed]