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

from google.api_core.exceptions import BadRequest

import pytest

from clouddq.integration.bigquery.bigquery_client import BigQueryClient


@pytest.mark.e2e
class TestBigQueryClient:

    @pytest.fixture(scope="session")
    def client(self):
        """Get BigQuery Client using discovered ADC"""
        client = BigQueryClient()
        yield client
        client.close_connection()

    def test_check_query_dry_run_failure(self, client):
        query = "SELECT 1; drop table students;--"
        with pytest.raises(BadRequest):
            client.check_query_dry_run(query)

    def test_check_query_dry_run(self, client):
        query = "SELECT 1 + 2"
        client.check_query_dry_run(query)

    def test_assert_is_dataset_exists(self, client):
        table_name = "bigquery-public-data.github_repos.commits"
        table_ref = client.table_from_string(
            table_name
        )
        project_id = table_ref.project
        dataset_id = table_ref.dataset_id
        assert client.is_dataset_exists(dataset=dataset_id, project_id=project_id) is True

    def test_assert_is_dataset_exists_implicit_project_id(self, client, gcp_dataplex_bigquery_dataset_id):
        assert client.is_dataset_exists(dataset=gcp_dataplex_bigquery_dataset_id) is True

    def test_assert_dataset_is_in_region(self, client):
        dataset = "bigquery-public-data.google_analytics_sample"
        project_id = dataset.split(".")[0]
        region = "US"
        assert region == client.get_dataset_region(dataset=dataset, project_id=project_id)

    def test_assert_dataset_is_in_region_failure(self, client):
        dataset = "bigquery-public-data.google_analytics_sample"
        project_id = dataset.split(".")[0]
        region = "EU"
        assert region != client.get_dataset_region(dataset=dataset, project_id=project_id)

    def test_assert_is_table_exists(self, client):
        table_name = "bigquery-public-data.github_repos.commits"
        table_ref = client.table_from_string(
            table_name
        )
        project_id = table_ref.project
        assert client.is_table_exists(table=table_name, project_id=project_id) is True

    def test_assert_is_table_exists_implicit_project_id(self,
            client,
            gcp_project_id,
            target_bq_result_dataset_name,
            target_bq_result_table_name):
        table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
        assert client.is_table_exists(table=table) is True

    def test_assert_required_columns_exist_in_table(self,
            client,
            gcp_project_id,
            target_bq_result_dataset_name,
            target_bq_result_table_name):
        table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
        client.assert_required_columns_exist_in_table(table=table, project_id=gcp_project_id)

    def test_assert_required_columns_exist_in_table_implicit_project_id(self,
            client,
            gcp_project_id,
            target_bq_result_dataset_name,
            target_bq_result_table_name):
        table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
        client.assert_required_columns_exist_in_table(table=table)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
