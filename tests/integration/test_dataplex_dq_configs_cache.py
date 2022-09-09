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


from pathlib import Path

import logging
import shutil
import sys

import pytest

from clouddq import lib
from clouddq.classes.dq_configs_cache import DqConfigsCache
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

@pytest.mark.dataplex
class TestDqConfigsCache:

    def test_prepare_configs_cache(self, temp_configs_dir, tmp_path):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_configs_cache_1")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                configs_cache = lib.prepare_configs_cache(temp_configs_dir)
                assert type(configs_cache) == DqConfigsCache
                assert configs_cache._cache_db["entities"].exists()
                assert configs_cache._cache_db["rules"].exists()
                assert configs_cache._cache_db["row_filters"].exists()
                assert configs_cache._cache_db["rule_bindings"].exists()
                assert configs_cache._cache_db["reference_columns"].exists()
        finally:
            shutil.rmtree(temp_dir)

    def test_resolve_dataplex_entity_uris(self,
            temp_configs_dir,
            test_dq_dataplex_client,
            test_bigquery_client,
            test_dataplex_metadata_defaults_configs,
            tmp_path):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_configs_cache_2")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                configs_cache = lib.prepare_configs_cache(temp_configs_dir)
                count_1 = configs_cache._cache_db['entities'].count
                target_rule_binding_ids = [
                    row["id"].lower() for row in
                    configs_cache._cache_db.query("select id from rule_bindings")
                ]
                configs_cache.resolve_dataplex_entity_uris(
                    dataplex_client=test_dq_dataplex_client,
                    bigquery_client=test_bigquery_client,
                    default_configs=test_dataplex_metadata_defaults_configs,
                    target_rule_binding_ids=target_rule_binding_ids
                )
                count_2 = configs_cache._cache_db['entities'].count
                assert count_2 > count_1
        finally:
            shutil.rmtree(temp_dir)

    def test_get_entities_configs_from_rule_bindings(
        self,
        test_default_dataplex_configs_cache,
        gcp_project_id,
        gcp_dataplex_bigquery_dataset_id,
        tmp_path):
        try:
            temp_dir = Path(tmp_path).joinpath("clouddq_test_configs_cache_3")
            temp_dir.mkdir(parents=True)
            with working_directory(temp_dir):
                rule_binding_ids = [
                    'T5_URI_BQ_EMAIL_DUPLICATE',
                    'T7_URI_DP_EMAIL_DUPLICATE'
                ]
                output = test_default_dataplex_configs_cache.\
                    get_entities_configs_from_rule_bindings(rule_binding_ids)
                table_name = (
                    f'{gcp_project_id.replace("-","_")}__{gcp_dataplex_bigquery_dataset_id}'
                    '__contact_details__VALUE_1'
                )
                expected = {
                    table_name: {
                        'rule_binding_ids_list': [
                            'T5_URI_BQ_EMAIL_DUPLICATE',
                            'T7_URI_DP_EMAIL_DUPLICATE'
                        ]
                    }
                }
                assert expected == output
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto'] + sys.argv[1:]))
