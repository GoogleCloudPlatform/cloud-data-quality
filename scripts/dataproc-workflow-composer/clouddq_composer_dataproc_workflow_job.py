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

"""
CloudDQ PySpark with Dataproc Workflow job scheduling using Composer
"""

from datetime import datetime, timedelta
import os

from airflow import models
from airflow.contrib.operators.dataproc_operator import (
    DataprocWorkflowTemplateInstantiateOperator
)
from airflow.utils import trigger_rule


PROJECT_ID = "<your_gcp_project_id>"
REGION = "<preferred_gcp_region>"
TEMPLATE_ID = "<template_id>"

SCHEDULE_INTERVAL = timedelta(minutes=60)
START_DATE = datetime.now() - SCHEDULE_INTERVAL

# [START composer_simple_define_dag_airflow_1]
default_dag_args = {
    'start_date': START_DATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': PROJECT_ID
}

# Define a DAG (directed acyclic graph) of tasks.
with models.DAG(
    # The id you will see in the DAG airflow page
    'dataproc_workflow_clouddq',
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_dag_args
) as dag:
    start_template_job = DataprocWorkflowTemplateInstantiateOperator(
        # The task id of your job
        task_id="dataproc_workflow_clouddq",
        # The template id of your workflow
        template_id=TEMPLATE_ID,
        project_id=PROJECT_ID,
        # The region for the template
        # For more info on regions where Dataflow is available see:
        # https://cloud.google.com/dataflow/docs/resources/locations
        region=REGION,
    )