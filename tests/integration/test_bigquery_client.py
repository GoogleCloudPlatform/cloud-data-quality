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

from google.api_core.exceptions import BadRequest

import pytest


@pytest.mark.e2e
class TestBigQueryClient:

    def test_check_query_dry_run_failure(self, test_bigquery_client):
        query = "SELECT 1; drop table students;--"
        with pytest.raises(BadRequest):
            test_bigquery_client.check_query_dry_run(query)

    def test_check_query_dry_run(self, test_bigquery_client):
        query = "SELECT 1 + 2"
        test_bigquery_client.check_query_dry_run(query)

    def test_assert_is_dataset_exists(self, test_bigquery_client):
        table_name = "bigquery-public-data.github_repos.commits"
        table_ref = test_bigquery_client.table_from_string(
            table_name
        )
        dataset_id = table_ref.dataset_id
        assert test_bigquery_client.is_dataset_exists(dataset=dataset_id) is True

    def test_assert_is_dataset_exists_implicit_project_id(self, test_bigquery_client, gcp_dataplex_bigquery_dataset_id):
        assert test_bigquery_client.is_dataset_exists(dataset=gcp_dataplex_bigquery_dataset_id) is True

    def test_assert_dataset_is_in_region(self, test_bigquery_client):
        dataset = "bigquery-public-data.google_analytics_sample"
        region = "US"
        assert region == test_bigquery_client.get_dataset_region(dataset=dataset)

    def test_assert_dataset_is_in_region_failure(self, test_bigquery_client):
        dataset = "bigquery-public-data.google_analytics_sample"
        region = "EU"
        assert region != test_bigquery_client.get_dataset_region(dataset=dataset)

    def test_assert_is_table_exists(self, test_bigquery_client):
        table_name = "bigquery-public-data.github_repos.commits"
        assert test_bigquery_client.is_table_exists(table=table_name) is True

    def test_assert_is_table_exists_fail(self, test_bigquery_client):
        table_name = "bigquery-public-data.github_repos.commits_fail"
        assert test_bigquery_client.is_table_exists(table=table_name) is False

    def test_assert_is_table_exists_implicit_project_id(self,
            test_bigquery_client,
            gcp_project_id,
            target_bq_result_dataset_name,
            target_bq_result_table_name):
        table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
        assert test_bigquery_client.is_table_exists(table=table) is True

    def test_assert_required_columns_exist_in_table(self,
            test_bigquery_client,
            gcp_project_id,
            target_bq_result_dataset_name,
            target_bq_result_table_name):
        table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
        test_bigquery_client.assert_required_columns_exist_in_table(table=table, project_id=gcp_project_id)

    def test_assert_required_columns_exist_in_table_implicit_project_id(self,
            test_bigquery_client,
            gcp_project_id,
            target_bq_result_dataset_name,
            target_bq_result_table_name):
        table = f"{gcp_project_id}.{target_bq_result_dataset_name}.{target_bq_result_table_name}"
        test_bigquery_client.assert_required_columns_exist_in_table(table=table)

    def test_get_table_schema(self,
        gcp_project_id,
        test_bigquery_client):
        table_name = "bigquery-public-data.github_repos.commits"
        table_ref = test_bigquery_client.table_from_string(
            table_name
        )
        project_id = table_ref.project
        is_table_exists = test_bigquery_client.is_table_exists(
            table=table_name,
        )
        if is_table_exists:
            table_schema = test_bigquery_client.get_table_schema(
                table=table_name,
            )
            expected_table_schema = {
                'columns': {
                    'COMMIT': {'name': 'commit', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'TREE': {'name': 'tree', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'PARENT': {'name': 'parent', 'type': 'STRING', 'mode': 'REPEATED', 'data_type': 'STRING'},
                    'AUTHOR': {'name': 'author', 'type': 'RECORD', 'mode': 'NULLABLE', 'data_type': 'RECORD'},
                    'COMMITTER': {'name': 'committer', 'type': 'RECORD', 'mode': 'NULLABLE', 'data_type': 'RECORD'},
                    'SUBJECT': {'name': 'subject', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'MESSAGE': {'name': 'message', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'TRAILER': {'name': 'trailer', 'type': 'RECORD', 'mode': 'REPEATED', 'data_type': 'RECORD'},
                    'DIFFERENCE': {'name': 'difference', 'type': 'RECORD', 'mode': 'REPEATED', 'data_type': 'RECORD'},
                    'DIFFERENCE_TRUNCATED': {'name': 'difference_truncated', 'type': 'BOOLEAN', 'mode': 'NULLABLE',
                                             'data_type': 'BOOLEAN'},
                    'REPO_NAME': {'name': 'repo_name', 'type': 'STRING', 'mode': 'REPEATED', 'data_type': 'STRING'},
                    'ENCODING': {'name': 'encoding', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'}
                },
                'partition_fields': None
            }
            assert table_schema == expected_table_schema
        else:
            print(f'`{table_name}` does not exists in the project `{project_id}`')

    def test_get_table_schema_with_partition_fields(self,
        gcp_project_id,
        test_bigquery_client):
        table_name = f"{gcp_project_id}.austin_311.contact_details_partitioned"
        table_ref = test_bigquery_client.table_from_string(
            table_name
        )
        project_id = table_ref.project
        is_table_exists = test_bigquery_client.is_table_exists(
            table=table_name,
        )
        if is_table_exists:
            table_schema = test_bigquery_client.get_table_schema(
                table=table_name,
            )
            expected_table_schema = {
                'columns': {
                    'ROW_ID': {'name': 'row_id', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'CONTACT_TYPE': {'name': 'contact_type', 'type': 'STRING', 'mode': 'NULLABLE',
                                     'data_type': 'STRING'},
                    'VALUE': {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'TS': {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE', 'data_type': 'TIMESTAMP'}
                },
                'partition_fields': [{'name': 'ts', 'type': 'TIMESTAMP', 'partitioning_type': 'DAY'}]
            }
            assert table_schema == expected_table_schema
        else:
            print(f'`{table_name}` does not exists in the project `{project_id}`')

    def test_get_table_schema_with_default_partitioning(self,
        gcp_project_id,
        test_bigquery_client):
        table_name = f"{gcp_project_id}.austin_311.contact_details_ingestion_time_partitioned"
        table_ref = test_bigquery_client.table_from_string(
            table_name
        )
        project_id = table_ref.project
        is_table_exists = test_bigquery_client.is_table_exists(
            table=table_name,
        )
        if is_table_exists:
            table_schema = test_bigquery_client.get_table_schema(
                table=table_name,
            )
            expected_table_schema = {
                'columns': {
                    'ROW_ID': {'name': 'row_id', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'CONTACT_TYPE': {'name': 'contact_type', 'type': 'STRING', 'mode': 'NULLABLE',
                                     'data_type': 'STRING'},
                    'VALUE': {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE', 'data_type': 'STRING'},
                    'TS': {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE', 'data_type': 'TIMESTAMP'}
                },
                'partition_fields': [{'name': '_PARTITIONTIME', 'partitioning_type': 'DAY', 'type': 'TIMESTAMP'}]
            }
            assert table_schema == expected_table_schema
        else:
            print(f'`{table_name}` does not exists in the project `{project_id}`')

    def test_get_table_columns(self,
        gcp_project_id,
        test_bigquery_client):
        table_name = "bigquery-public-data.github_repos.commits"
        table_ref = test_bigquery_client.table_from_string(
            table_name
        )
        project_id = table_ref.project
        is_table_exists = test_bigquery_client.is_table_exists(
            table=table_name,
        )
        if is_table_exists:
            table_columns = test_bigquery_client.get_table_columns(
                table=table_name,
            )
            expected_table_columns = {
                    'commit', 'tree', 'parent', 'author', 'committer', 'subject', 'message',
                    'trailer', 'difference', 'difference_truncated', 'repo_name', 'encoding'
            }
            assert table_columns == expected_table_columns
        else:
            print(f'`{table_name}` does not exists in the project `{project_id}`')


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
