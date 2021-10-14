#!/bin/bash
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

export GOOGLE_CLOUD_PROJECT="<your-project-id>"
export CLOUDDQ_BIGQUERY_DATASET="<your-bigquery-dataset-id>"
export CLOUDDQ_BIGQUERY_REGION="<gcp-region-for-bigquery-jobs>"
export GOOGLE_APPLICATION_CREDENTIALS="<path-to-exported-service-account-key>"
export IMPERSONATION_SERVICE_ACCOUNT="<service-account-name-for-impersonation>"