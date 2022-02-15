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

from pathlib import Path
from pprint import pformat

import json
import logging
import re
import sys

import pytest

from clouddq import lib
from clouddq import utils
from clouddq.classes.dq_rule_binding import DqRuleBinding


RE_NEWLINES = r"(\n( )*)+"
RE_CONFIGS_HASHSUM = r"'[\w\d]+' AS configs_hashsum,"
RE_ASSET_ID = r"'[\w-]+' AS dataplex_asset_id,"
CONFIGS_HASHSUM_REP = "'' AS configs_hashsum,"
ASSET_ID_REP = "'<your_dataplex_asset_id>' AS dataplex_asset_id,"

logger = logging.getLogger(__name__)

@pytest.mark.dataplex
class TestDataplexMetadataUriTemplates:
    """ """
    @pytest.fixture(scope="function")
    def test_rule_bindings_collection_team_4(self, temp_configs_dir):
        """ """
        return lib.load_rule_bindings_config(
            Path(temp_configs_dir / "rule_bindings/team-4-rule-bindings.yml")
        )

    @pytest.fixture(scope="function")
    def test_rule_bindings_collection_team_5(self, temp_configs_dir):
        """ """
        return lib.load_rule_bindings_config(
            Path(temp_configs_dir / "rule_bindings/team-5-rule-bindings.yml")
        )

    @pytest.fixture(scope="function")
    def test_rule_bindings_collection_team_6(self, temp_configs_dir):
        """ """
        return lib.load_rule_bindings_config(
            Path(temp_configs_dir / "rule_bindings/team-6-rule-bindings.yml")
        )

    @pytest.fixture(scope="function")
    def test_rule_bindings_collection_from_configs_file(self, temp_configs_from_file):
        """ """
        return lib.load_rule_bindings_config(
            Path(temp_configs_from_file)
        )

    def test_rule_bindings_class_resolve_configs(
        self,
        test_rule_bindings_collection_team_4,
        test_default_dataplex_configs_cache,
        test_dataplex_metadata_defaults_configs,
    ):
        for key, value in test_rule_bindings_collection_team_4.items():
            rule_binding = DqRuleBinding.from_dict(
                rule_binding_id=key,
                kwargs=value,
                default_configs=test_dataplex_metadata_defaults_configs)
            rule_binding.resolve_table_entity_config(configs_cache=test_default_dataplex_configs_cache)
            rule_binding.resolve_rule_config_list(configs_cache=test_default_dataplex_configs_cache)
            rule_binding.resolve_row_filter_config(configs_cache=test_default_dataplex_configs_cache)
            rule_binding.resolve_all_configs_to_dict(configs_cache=test_default_dataplex_configs_cache)

    def test_prepare_configs_from_rule_binding(
        self,
        test_rule_bindings_collection_team_4,
        test_default_dataplex_configs_cache,
        test_resources,
        test_dataplex_metadata_defaults_configs,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_4.items():

            env = "DEV"
            metadata = {"channel": "two"}
            configs = lib.prepare_configs_from_rule_binding_id(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                environment=env,
                metadata=metadata,
                configs_cache=test_default_dataplex_configs_cache,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            logger.info(pformat(json.dumps(configs["configs"])))

            configs["configs"]["rule_binding_id"] = "<dataplex_rule_binding_id>"
            configs["configs"]["entity_id"] = "<dataplex_entity_id>"
            configs["configs"]["entity_configs"]["database_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["instance_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataset_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["project_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataplex_name"] = "<your-gcp-dataplex-name>"
            configs["configs"]["entity_configs"]["dataplex_lake"] = "<your-gcp-dataplex-lake-id>"
            configs["configs"]["entity_configs"]["dataplex_zone"] = "<your-gcp-dataplex-zone-id>"
            configs["configs"]["entity_configs"]["dataplex_location"] = "<your-gcp-dataplex-region-id>"
            configs["configs"]["entity_configs"]["dataplex_asset_id"] = "<your-gcp-dataplex-asset-id>"
            configs["configs"]["entity_configs"]["dataplex_createTime"] = "<dataplex_entity_createTime>"

            with open(test_resources / "dataplex_metadata_expected_configs.json") as f:
                expected_configs = json.loads(f.read())
                assert configs["configs"] == dict(expected_configs)
            metadata.update(rule_binding_configs["metadata"])
            assert configs["metadata"] == dict(metadata)
            assert configs["environment"] == env

    def test_render_run_dq_main_sql(
        self,
        test_rule_bindings_collection_team_4,
        test_default_dataplex_configs_cache,
        test_resources,
        gcp_project_id,
        gcp_dataplex_bigquery_dataset_id,
        gcp_bq_dataset,
        test_dataplex_metadata_defaults_configs,
        gcp_dataplex_zone_id,
        gcp_dataplex_lake_name,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_4.items():
            with open(test_resources / "dataplex_metadata_sql_expected.sql") as f:
                expected = f.read()
            output = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                configs_cache=test_default_dataplex_configs_cache,
                environment="DEV",
                debug=True,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            output = output.replace(gcp_project_id, "<your-gcp-project-id>")\
                .replace(gcp_dataplex_bigquery_dataset_id, "<your_bigquery_dataset_id>")\
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
            output = output.replace("CAST(NULL AS STRING) AS dataplex_asset_id,", ASSET_ID_REP)
            expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
            assert output == expected

    def test_rule_bindings_class_resolve_configs_from_file(
            self,
            test_default_dataplex_configs_cache_from_file,
            test_dataplex_metadata_defaults_configs,
            test_rule_bindings_collection_from_configs_file,
    ):
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_from_configs_file.items():
            rule_binding = DqRuleBinding.from_dict(
                rule_binding_id=rule_binding_id,
                kwargs=rule_binding_configs,
                default_configs=test_dataplex_metadata_defaults_configs)
            rule_binding.resolve_table_entity_config(configs_cache=test_default_dataplex_configs_cache_from_file)
            rule_binding.resolve_rule_config_list(configs_cache=test_default_dataplex_configs_cache_from_file)
            rule_binding.resolve_row_filter_config(configs_cache=test_default_dataplex_configs_cache_from_file)
            rule_binding.resolve_all_configs_to_dict(configs_cache=test_default_dataplex_configs_cache_from_file)

    def test_prepare_configs_from_configs_file(
            self,
            test_rule_bindings_collection_from_configs_file,
            test_default_dataplex_configs_cache_from_file,
            test_resources,
            test_dataplex_metadata_defaults_configs,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_from_configs_file.items():
            env = "DEV"
            metadata = {"channel": "two"}
            configs = lib.prepare_configs_from_rule_binding_id(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                environment=env,
                metadata=metadata,
                configs_cache=test_default_dataplex_configs_cache_from_file,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            logger.info(pformat(json.dumps(configs["configs"])))

            configs["configs"]["rule_binding_id"] = "<dataplex_rule_binding_id>"
            configs["configs"]["entity_id"] = "<dataplex_entity_id>"
            configs["configs"]["entity_configs"]["database_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["instance_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataset_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["project_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataplex_name"] = "<your-gcp-dataplex-name>"
            configs["configs"]["entity_configs"]["dataplex_lake"] = "<your-gcp-dataplex-lake-id>"
            configs["configs"]["entity_configs"]["dataplex_zone"] = "<your-gcp-dataplex-zone-id>"
            configs["configs"]["entity_configs"]["dataplex_location"] = "<your-gcp-dataplex-region-id>"
            configs["configs"]["entity_configs"]["dataplex_asset_id"] = "<your-gcp-dataplex-asset-id>"
            configs["configs"]["entity_configs"]["dataplex_createTime"] = "<dataplex_entity_createTime>"

            with open(test_resources / "dataplex_metadata_expected_configs.json") as f:
                expected_configs = json.loads(f.read())
                assert configs["configs"] == dict(expected_configs)
            metadata.update(rule_binding_configs["metadata"])
            assert configs["metadata"] == dict(metadata)
            assert configs["environment"] == env

    def test_render_run_dq_main_sql_from_configs_file(
        self,
        test_rule_bindings_collection_from_configs_file,
        test_default_dataplex_configs_cache_from_file,
        test_resources,
        gcp_project_id,
        gcp_dataplex_bigquery_dataset_id,
        gcp_bq_dataset,
        test_dataplex_metadata_defaults_configs,
        gcp_dataplex_zone_id,
        gcp_dataplex_lake_name
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_from_configs_file.items():

            with open(test_resources / "dataplex_metadata_sql_expected.sql") as f:
                expected = f.read()
            output = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                configs_cache=test_default_dataplex_configs_cache_from_file,
                environment="DEV",
                debug=True,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
            output = output.replace(gcp_project_id, "<your-gcp-project-id>")\
                .replace(gcp_dataplex_bigquery_dataset_id, "<your_bigquery_dataset_id>")\
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
            expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
            print(output)
            output = re.sub(RE_NEWLINES, '\n', output).strip()
            output = re.sub(RE_ASSET_ID, ASSET_ID_REP, output)
            output = output.replace("CAST(NULL AS STRING) AS dataplex_asset_id,", ASSET_ID_REP)
            assert output == expected

    def test_rule_bindings_class_resolve_gcs_configs(
        self,
        test_rule_bindings_collection_team_5,
        test_default_dataplex_configs_cache,
        test_dataplex_metadata_defaults_configs,
    ):
        for key, value in test_rule_bindings_collection_team_5.items():
            rule_binding = DqRuleBinding.from_dict(
                rule_binding_id=key,
                kwargs=value,
                default_configs=test_dataplex_metadata_defaults_configs)
            rule_binding.resolve_table_entity_config(configs_cache=test_default_dataplex_configs_cache)
            rule_binding.resolve_rule_config_list(configs_cache=test_default_dataplex_configs_cache)
            rule_binding.resolve_row_filter_config(configs_cache=test_default_dataplex_configs_cache)
            rule_binding.resolve_all_configs_to_dict(configs_cache=test_default_dataplex_configs_cache)

    def test_prepare_configs_from_gcs_rule_binding(
        self,
        test_rule_bindings_collection_team_5,
        test_default_dataplex_configs_cache,
        test_resources,
        test_dataplex_metadata_defaults_configs,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_5.items():

            env = "DEV"
            metadata = {"channel": "two"}
            configs = lib.prepare_configs_from_rule_binding_id(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                environment=env,
                metadata=metadata,
                configs_cache=test_default_dataplex_configs_cache,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            logger.info(pformat(json.dumps(configs["configs"])))

            configs["configs"]["rule_binding_id"] = "<dataplex_rule_binding_id>"
            configs["configs"]["entity_id"] = "<dataplex_entity_id>"
            configs["configs"]["entity_configs"]["database_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["instance_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataset_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["project_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataplex_name"] = "<your-gcp-dataplex-name>"
            configs["configs"]["entity_configs"]["dataplex_lake"] = "<your-gcp-dataplex-lake-id>"
            configs["configs"]["entity_configs"]["dataplex_zone"] = "<your-gcp-dataplex-zone-id>"
            configs["configs"]["entity_configs"]["dataplex_location"] = "<your-gcp-dataplex-region-id>"
            configs["configs"]["entity_configs"]["dataplex_asset_id"] = "<your-gcp-dataplex-asset-id>"
            configs["configs"]["entity_configs"]["dataplex_createTime"] = "<dataplex_entity_createTime>"

            with open(test_resources / "dataplex_gcs_metadata_expected_configs.json") as f:
                expected_configs = json.loads(f.read())
                assert configs["configs"] == dict(expected_configs)
            metadata.update(rule_binding_configs["metadata"])
            assert configs["metadata"] == dict(metadata)
            assert configs["environment"] == env

    def test_render_run_dq_main_sql_gcs(
        self,
        test_rule_bindings_collection_team_5,
        test_default_dataplex_configs_cache,
        test_resources,
        gcp_project_id,
        test_dataplex_metadata_defaults_configs,
        gcp_dataplex_zone_id,
        gcp_dataplex_lake_name,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_5.items():

            with open(test_resources / "dataplex_gcs_metadata_sql_expected.sql") as f:
                expected = f.read()
            output = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                configs_cache=test_default_dataplex_configs_cache,
                environment="DEV",
                debug=True,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            output = output.replace(gcp_project_id, "<your-gcp-project-id>")\
                .replace(gcp_dataplex_zone_id.replace('-', '_'), "<your_dataplex_zone_name>")\
                .replace(gcp_dataplex_zone_id, "<your_dataplex_zone_name>")\
                .replace(rule_binding_id, "<rule_binding_id>")\
                .replace(gcp_dataplex_lake_name, "<your_dataplex_lake_id>")
            expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
            output = re.sub(RE_NEWLINES, '\n', output).strip()
            output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
            output = re.sub(RE_ASSET_ID, ASSET_ID_REP, output)
            assert output == expected

    def test_prepare_configs_from_gcs_partitioned_rule_binding(
        self,
        test_rule_bindings_collection_team_6,
        test_default_dataplex_configs_cache,
        test_resources,
        test_dataplex_metadata_defaults_configs,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_6.items():

            env = "DEV"
            metadata = {"channel": "two"}
            configs = lib.prepare_configs_from_rule_binding_id(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                environment=env,
                metadata=metadata,
                configs_cache=test_default_dataplex_configs_cache,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            logger.info(pformat(json.dumps(configs["configs"])))

            configs["configs"]["rule_binding_id"] = "<dataplex_rule_binding_id>"
            configs["configs"]["entity_id"] = "<dataplex_entity_id>"
            configs["configs"]["entity_configs"]["database_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["instance_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataset_name"] = "<your_dataplex_bigquery_dataset_id>"
            configs["configs"]["entity_configs"]["project_name"] = "<your-gcp-project-id>"
            configs["configs"]["entity_configs"]["dataplex_name"] = "<your-gcp-dataplex-name>"
            configs["configs"]["entity_configs"]["dataplex_lake"] = "<your-gcp-dataplex-lake-id>"
            configs["configs"]["entity_configs"]["dataplex_zone"] = "<your-gcp-dataplex-zone-id>"
            configs["configs"]["entity_configs"]["dataplex_location"] = "<your-gcp-dataplex-region-id>"
            configs["configs"]["entity_configs"]["dataplex_asset_id"] = "<your-gcp-dataplex-asset-id>"
            configs["configs"]["entity_configs"]["dataplex_createTime"] = "<dataplex_entity_createTime>"

            with open(test_resources / "dataplex_gcs_partitioned_metadata_expected_configs.json") as f:
                expected_configs = json.loads(f.read())
                assert configs["configs"] == dict(expected_configs)
            metadata.update(rule_binding_configs["metadata"])
            assert configs["metadata"] == dict(metadata)
            assert configs["environment"] == env

    def test_render_run_dq_main_sql_gcs_partitioned(
        self,
        test_rule_bindings_collection_team_6,
        test_default_dataplex_configs_cache,
        test_resources,
        gcp_project_id,
        test_dataplex_metadata_defaults_configs,
        gcp_dataplex_zone_id,
        gcp_dataplex_lake_name,
    ):
        """ """
        for rule_binding_id, rule_binding_configs in test_rule_bindings_collection_team_6.items():

            with open(test_resources / "dataplex_gcs_partitioned_metadata_sql_expected.sql") as f:
                expected = f.read()
            output = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
                configs_cache=test_default_dataplex_configs_cache,
                environment="DEV",
                debug=True,
                default_configs=test_dataplex_metadata_defaults_configs,
            )
            output = output.replace(gcp_project_id, "<your-gcp-project-id>")\
                .replace(gcp_dataplex_zone_id.replace('-', '_'), "<your_dataplex_zone_name>")\
                .replace(gcp_dataplex_zone_id, "<your_dataplex_zone_name>")\
                .replace(rule_binding_id, "<rule_binding_id>")\
                .replace(gcp_dataplex_lake_name, "<your_dataplex_lake_id>")
            expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
            output = re.sub(RE_NEWLINES, '\n', output).strip()
            output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
            output = re.sub(RE_ASSET_ID, ASSET_ID_REP, output)
            assert output == expected


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', '2'] + sys.argv[1:]))
