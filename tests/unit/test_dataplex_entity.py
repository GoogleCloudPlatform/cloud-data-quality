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

import pytest

from clouddq.classes.dataplex_entity import DataplexEntity
from clouddq.classes.dataplex_entity_schema import DataplexEntitySchema


class TestDataplexEntity:

    @pytest.fixture()
    def mock_valid_dataplex_input(self) -> dict:
        """Fixture that returns a static valid dataplex entity."""
        with open("tests/resources/mock_valid_dataplex_entity.json") as f:
            return json.load(f)

    @pytest.fixture()
    def mock_invalid_dataplex_input(self) -> dict:
        """Fixture that returns a static invalid dataplex entity."""
        with open("tests/resources/mock_invalid_dataplex_entity.json") as f:
            return json.load(f)

    def test_create_dataplex_entity_from_input_dict(self, mock_valid_dataplex_input):
        """ """
        dataplex_entity_actual = DataplexEntity.from_dict(mock_valid_dataplex_input)

        dataplex_entity_expected_dict = {
            "name": "projects/project-id/locations/location-id/lakes/lake_name/zones/zone-id/entities/entity_id",
            "createTime": "createTimestamp",
            "updateTime": "updateTimestamp",
            "id": "entity_id",
            "type": "TABLE",
            "asset": "asset-id",
            "dataPath": "projects/project-id/datasets/bigquery_dataset_id/tables/table_name",
            "system": "BIGQUERY",
            "format": {"format": "OTHER"},
            "schema": {"fields": [{"name": "column1", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column2", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column3", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column4", "type": "TIMESTAMP", "mode": "REQUIRED"}]}
        }
        assert dataplex_entity_actual == DataplexEntity.from_dict(dataplex_entity_expected_dict)

    def test_validate_dataplex_entity(self, mock_valid_dataplex_input):
        """ """
        dataplex_entity = DataplexEntity.from_dict(mock_valid_dataplex_input)

        schema = {"fields": [{"name": "column1", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column2", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column3", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column4", "type": "TIMESTAMP", "mode": "REQUIRED"}]}

        assert dataplex_entity.name == "projects/project-id/locations/location-id/lakes/" \
                                       "lake_name/zones/zone-id/entities/entity_id"
        assert dataplex_entity.createTime == "createTimestamp"
        assert dataplex_entity.updateTime == "updateTimestamp"
        assert dataplex_entity.id == "entity_id"
        assert dataplex_entity.type == "TABLE"
        assert dataplex_entity.asset == "asset-id"
        assert dataplex_entity.dataPath == "projects/project-id/datasets/bigquery_dataset_id/tables/table_name"
        assert dataplex_entity.system == "BIGQUERY"
        assert dataplex_entity.format == {"format": "OTHER"}
        assert dataplex_entity.schema == DataplexEntitySchema.from_dict(kwargs=schema)
        assert dataplex_entity.project_id == "project-id"
        assert dataplex_entity.location == "location-id"
        assert dataplex_entity.lake == "lake_name"
        assert dataplex_entity.zone == "zone-id"

    def test_validate_dataplex_entity_to_dict(self, mock_valid_dataplex_input):
        """ """
        dataplex_entity = DataplexEntity.from_dict(mock_valid_dataplex_input)

        schema = {"fields": [{"name": "column1", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column2", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column3", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "column4", "type": "TIMESTAMP", "mode": "REQUIRED"}]}

        dataplex_entity_expected = {
            "name": "projects/project-id/locations/location-id/lakes/lake_name/zones/zone-id/entities/entity_id",
            "createTime": "createTimestamp",
            "updateTime": "updateTimestamp",
            "id": "entity_id",
            "type": "TABLE",
            "asset": "asset-id",
            "dataPath": "projects/project-id/datasets/bigquery_dataset_id/tables/table_name",
            'db_primary_key': f'projects/project-id/locations/location-id/lakes'
                              f'/lake_name/zones/zone-id/entities/entity_id',
            "system": "BIGQUERY",
            "format": {"format": "OTHER"},
            "schema": DataplexEntitySchema.from_dict(kwargs=schema),
            "project_id": "project-id",
            "location": "location-id",
            "lake": "lake_name",
            "zone": "zone-id",
        }

        assert dataplex_entity.to_dict() == dataplex_entity_expected

    def test_dataplex_entity_missing_columns_failure(self, mock_invalid_dataplex_input):
        """ """
        with pytest.raises(ValueError):
            DataplexEntity.from_dict(kwargs=mock_invalid_dataplex_input)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
