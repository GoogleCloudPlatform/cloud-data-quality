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

import pytest

from clouddq.classes.dataplex_entity import DataplexEntity
from clouddq.classes.dq_entity_uri import EntityUri


class TestEntityURI:

    @pytest.mark.parametrize(
        "entity_uri,error_type",
        [
            pytest.param(
                "dataplex://",
                ValueError,
                id="incomplete_dataplex"
            ),
            pytest.param(
                "bigquery://",
                NotImplementedError,
                id="incomplete_bigquery"
            ),
            pytest.param(
                "local://",
                NotImplementedError,
                id="incomplete_local"
            ),
            pytest.param(
                "gs://",
                NotImplementedError,
                id="not_implemented_scheme"
            ),
            pytest.param(
                "dataplex:bigquery://",
                ValueError,
                id="invalid_scheme"
            ),
            pytest.param(
                "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities",
                ValueError,
                id="missing_entity_id"
            ),
            pytest.param(
                "dataplex://projects@",
                ValueError,
                id="unsupported_@"
            ),
            pytest.param(
                "dataplex://projects:",
                ValueError,
                id="unsupported_:"
            ),
            pytest.param(
                "dataplex://projects?",
                ValueError,
                id="unsupported_?"
            ),
            pytest.param(
                "dataplex://projects#",
                ValueError,
                id="unsupported_#"
            ),
        ],
    )
    def test_entity_uri_parse_failure(self, entity_uri, error_type):
        """ """
        with pytest.raises(error_type):
            EntityUri.from_uri(entity_uri)

    def test_entity_uri_parse_dataplex_uri(self):
        """ """
        entity_uri = "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id"
        parsed_uri = EntityUri.from_uri(entity_uri)
        expected_entity_dict = {
            "uri": "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id",
            "scheme": "dataplex",
            "entity_id": "entity-id",
            "configs": {
                "projects": "project-id",
                "locations": "us-central1",
                "lakes": "lake-id",
                "zones": "zone-id",
                "entities": "entity-id",
            }
        }
        assert parsed_uri.scheme == "dataplex"
        assert parsed_uri.uri == "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id"
        assert parsed_uri.entity_id == "entity-id"
        assert parsed_uri.configs == expected_entity_dict["configs"]
        assert parsed_uri.to_dict() == expected_entity_dict

    def test_entity_uri_dataplex_uri_to_dataplex_entity(self):
        entity_uri = "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id"
        parsed_uri = EntityUri.from_uri(entity_uri)
        dataplex_entity = DataplexEntity.from_uri(parsed_uri)

        expected_dataplex_entity_dict = {
            "name": "projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id",
            "createTime": None,
            "updateTime": None,
            "id": "entity-id",
            "project_id": "project-id",
            "location": "us-central1",
            "lake": "lake-id",
            "zone": "zone-id",
            "type": None,
            "asset": None,
            "dataPath": None,
            "system": None,
            "format": None,
            "schema": None,
        }
        assert dataplex_entity.to_dict() == expected_dataplex_entity_dict

    @pytest.mark.parametrize(
        "entity_uri,error_type",
        [
            pytest.param(
                "dataplex://project/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id",
                ValueError,
                id="typo_project"
            ),
            pytest.param(
                "dataplex://projects/project-id/location/us-central1/lakes/lake-id/zones/zone-id/entities/entity-id",
                ValueError,
                id="typo_location"
            ),
            pytest.param(
                "dataplex://projects/project-id/locations/us-central1/lake/lake-id/zones/zone-id/entities/entity-id",
                ValueError,
                id="typo_lake"
            ),
            pytest.param(
                "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zone/zone-id/entities/entity-id",
                ValueError,
                id="typo_zone"
            ),
            pytest.param(
                "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entity/entity-id",
                ValueError,
                id="typo_entity"
            ),
            pytest.param(
                "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entity/entity-id",
                ValueError,
                id="typo_entity"
            ),
        ],
    )
    def test_entity_uri_dataplex_uri_to_dataplex_entity_failure(self, entity_uri, error_type):

        with pytest.raises(error_type):
            parsed_uri = EntityUri.from_uri(entity_uri)
            DataplexEntity.from_uri(parsed_uri)

    # def test_entity_uri_parse_elide_project_lake_id_failure(self):
    #     """ """
    #     entity_uri = "dataplex:///zones/zone-id/entities/entity-id"
    #     parsed_uri = EntityUri.from_uri(entity_uri)
    #     # This should be supported eventually
    #     with pytest.raises(NotImplementedError):
    #         DataplexEntity.from_uri(parsed_uri)

    # def test_entity_uri_parse_glob_failure(self):
    #     """ """
    #     entity_uri = "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/entity/test_entity_*"
    #     parsed_uri = EntityUri.from_uri(entity_uri)
    #     # This should be supported eventually
    #     with pytest.raises(NotImplementedError):
    #         DataplexEntity.from_uri(parsed_uri)

    # def test_entity_uri_parse_asset_id_failure(self):
    #     """ """
    #     entity_uri = "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/assets/asset-id"
    #     parsed_uri = EntityUri.from_uri(entity_uri)
    #     with pytest.raises(ValueError):
    #         DataplexEntity.from_uri(parsed_uri)

    # def test_entity_uri_parse_partition_failure(self):
    #     """ """
    #     entity_uri = "dataplex://projects/project-id/locations/us-central1/lakes/lake-id/zones/zone-id/partitions/partition-id"
    #     parsed_uri = EntityUri.from_uri(entity_uri)
    #     with pytest.raises(ValueError):
    #         DataplexEntity.from_uri(parsed_uri)

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n 2']))