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
from clouddq.integration.dataplex.clouddq_dataplex import CloudDqDataplexClient
import logging

logger = logging.getLogger(__name__)

class TestMetadataIntegration:

    @pytest.fixture
    def test_dq_dataplex_client(self,
                                dataplex_endpoint,
                                gcp_dataplex_lake_name,
                                gcp_dataplex_region,
                                gcp_project_id,
                                gcs_bucket_name):
        gcp_project_id = gcp_project_id
        gcs_bucket_name = gcs_bucket_name
        yield CloudDqDataplexClient(dataplex_endpoint=dataplex_endpoint,
                                    gcp_dataplex_lake_name=gcp_dataplex_lake_name,
                                    gcp_dataplex_region=gcp_dataplex_region,
                                    gcp_project_id=gcp_project_id,
                                    gcs_bucket_name=gcs_bucket_name)

    @pytest.mark.parametrize(
        "entity_id,params",
        [
            pytest.param(
                'contact_details',
                {"view": "FULL"},
                id="contact_details_with_full_entity_view"
            ),
            pytest.param(
                'contact_details',
                None,
                id="contact_details_with_default_entity_view"
            ),
            pytest.param(
                'asset_bucket',
                {"view": "FULL"},
                id="asset_bucket_with_full_entity_view"
            ),
            pytest.param(
                'asset_bucket',
                None,
                id="asset_bucket_with_default_entity_view"
            ),
        ]
    )
    def test_dataplex_metadata_get_entity(self,
                                            test_dq_dataplex_client,
                                            gcp_dataplex_zone_id,
                                            entity_id,
                                            params,):

        response = test_dq_dataplex_client.get_entity(zone_id=gcp_dataplex_zone_id,
                                                      entity_id=entity_id,
                                                      params=params)
        assert response.status_code == 200
        print(response.text)

    def test_dataplex_metadata_list_entities(self,
                                             test_dq_dataplex_client,
                                             gcp_dataplex_zone_id,):
        print(gcp_dataplex_zone_id)
        response = test_dq_dataplex_client.list_entities(zone_id=gcp_dataplex_zone_id)
        assert response.status_code == 200
        print(response.text)

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP']))
