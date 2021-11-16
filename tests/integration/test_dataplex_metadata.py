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
import logging
import json

logger = logging.getLogger(__name__)

class TestMetadataIntegration:

    @pytest.mark.parametrize(
        "entity_id,expected",
        [
            pytest.param(
                'contact_details',
                {
                    "name": "projects/dataplex-clouddq/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entities/contact_details",
                    "createTime": "2021-11-11T07:37:14.212950Z", "updateTime": "2021-11-11T07:37:14.212950Z",
                    "id": "contact_details", "type": "TABLE", "asset": "clouddq-test-asset-curated-bigquery",
                    "dataPath": "projects/dataplex-clouddq/datasets/clouddq_test_asset_curated/tables/contact_details",
                    "system": "BIGQUERY", "format": {"format": "OTHER"}, "schema": {
                    "fields": [{"name": "row_id", "type": "STRING", "mode": "REQUIRED"},
                               {"name": "contact_type", "type": "STRING", "mode": "REQUIRED"},
                               {"name": "value", "type": "STRING", "mode": "REQUIRED"},
                               {"name": "ts", "type": "TIMESTAMP", "mode": "REQUIRED"}]}},
                id="contact_details_with_full_entity_view"
            ),
            pytest.param(
                'asset_bucket',
                {
                    "name": "projects/168141520116/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entities/d6d0e5bc-163c-4993-8da2-68b4bad58633",
                    "createTime": "2021-10-25T02:35:36.207049Z", "updateTime": "2021-10-25T02:35:36.207049Z",
                    "id": "asset_bucket", "type": "TABLE", "asset": "asset-bucket",
                    "dataPath": "gs://amandeep-dev-bucket", "system": "CLOUD_STORAGE",
                    "format": {"format": "CSV", "mimeType": "text/csv",
                               "csv": {"encoding": "UTF-8", "headerRows": 1, "delimiter": ","}}, "schema": {
                    "fields": [{"name": "row_id", "type": "STRING", "mode": "NULLABLE"},
                               {"name": "contact_type", "type": "STRING", "mode": "NULLABLE"},
                               {"name": "value", "type": "STRING", "mode": "NULLABLE"},
                               {"name": "ts", "type": "STRING", "mode": "NULLABLE"}]}},
                id="asset_bucket_with_full_entity_view"
            ),
        ]
    )
    def test_dataplex_metadata_get_entity_valid(self,
                                            test_dq_dataplex_client,
                                            gcp_dataplex_zone_id,
                                            entity_id,
                                            expected,):

        response = test_dq_dataplex_client.get_dataplex_entity(zone_id=gcp_dataplex_zone_id,
                                                      entity_id=entity_id,)
        assert response.status_code == 200
        print(response.text)

        expected_obj = DataplexEntity.from_dict(kwargs=expected)

        actual_obj: DataplexEntity = DataplexEntity.from_dict(kwargs=json.loads(response.text))

        print("Response Object is")
        print(actual_obj)

        assert actual_obj == expected_obj

    def test_dataplex_metadata_list_entities(self,
                                             test_dq_dataplex_client,
                                             gcp_dataplex_zone_id,):
        print(f"zone id is {gcp_dataplex_zone_id}")
        response = test_dq_dataplex_client.list_dataplex_entities(zone_id=gcp_dataplex_zone_id)
        print(f"Response is \n {response}")
        print(f"Total Entities are {len(response['entities'])}")
        assert len(response['entities']) > 0

    def test_dataplex_metadata_list_entities_with_prefix(self,
                                             test_dq_dataplex_client,
                                             gcp_dataplex_zone_id,):
        print(f"zone id  is {gcp_dataplex_zone_id}")
        prefix = 'test_clouddq_'
        response = test_dq_dataplex_client.list_dataplex_entities(zone_id=gcp_dataplex_zone_id, prefix=prefix)
        print(f"Response is \n {response}")
        print(f"Total Entities are {len(response['entities'])}")
        assert len(response['entities']) > 0



if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
