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

import logging
import re

import pytest

from clouddq import lib
from clouddq import utils

RE_NEWLINES = r"(\n( )*)+"
RE_CONFIGS_HASHSUM = r"'[\w\d]+' AS configs_hashsum,"
CONFIGS_HASHSUM_REP = "'' AS configs_hashsum,"
RE_ASSET_ID = r"'[\w-]+' AS dataplex_asset_id,"
ASSET_ID_REP = "'<your_dataplex_asset_id>' AS dataplex_asset_id,"

logger = logging.getLogger(__name__)

class TestRowFilterParams:

    @pytest.fixture(scope="session")
    def test_rule_bindings_collection_team_10(self, source_configs_path):
        """ """
        return lib.load_rule_bindings_config(
            source_configs_path / "rule_bindings" / "team-10-rule-bindings.yml"
        )

    def test_dq_row_filter_params(
            self,
            test_rule_bindings_collection_team_10,
            test_configs_cache,
            test_resources,
            gcp_project_id,
            gcp_dataplex_bigquery_dataset_id,
            gcp_bq_dataset,
            gcp_dataplex_zone_id,
            gcp_dataplex_lake_name,
            test_bigquery_client,
    ):
        """

        Args:
          test_rule_bindings_collection_team_10:
          test_entities_collection:
          test_rules_collection:
          test_row_filters_collection:

        Returns:

        """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_10.items():
            with open(test_resources / "test_dq_row_filter_params_sql_expected.sql") as f:
                expected = f.read()
            configs = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                configs_cache=test_configs_cache,
                environment="DEV",
                debug=True,
                high_watermark_filter_exists=False,
                bigquery_client=test_bigquery_client,
            )
            output = configs.get("generated_sql_string_dict")\
                            .get(f"{rule_binding_id}_generated_sql_string")
            output = output.replace(gcp_project_id, "<your-gcp-project-id>") \
                .replace(gcp_dataplex_bigquery_dataset_id, "<your_bigquery_dataset_id>") \
                .replace(gcp_bq_dataset, "<your_bigquery_dataset_id>")
            if gcp_dataplex_zone_id in output:
                output = output.replace(gcp_dataplex_zone_id, "<your_dataplex_zone_id>")
            else:
                output = output.replace("CAST(NULL AS STRING) AS dataplex_zone",
                                        "'<your_dataplex_zone_id>' AS dataplex_zone")
            if gcp_dataplex_lake_name in output:
                output = output.replace(gcp_dataplex_lake_name, "<your_dataplex_lake_id>")
            else:
                output = output.replace("CAST(NULL AS STRING) AS dataplex_lake",
                                        "'<your_dataplex_lake_id>' AS dataplex_lake")

            output = output.replace(rule_binding_id, "<rule_binding_id>")
            output = re.sub(RE_NEWLINES, '\n', output).strip()
            output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
            output = re.sub(RE_ASSET_ID, ASSET_ID_REP, output)
            expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
            assert output == expected

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))