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

from pprint import pformat

import json
import logging
import re

import pytest

from clouddq import lib
from clouddq import utils
from clouddq.classes.dq_row_filter import DqRowFilter
from clouddq.classes.dq_rule import DqRule
from clouddq.classes.dq_rule_binding import DqRuleBinding
from clouddq.classes.rule_type import RuleType


logger = logging.getLogger(__name__)


RE_NEWLINES = r"(\n( )*)+"
RE_CONFIGS_HASHSUM = r"'[\w\d]+' AS configs_hashsum,"
CONFIGS_HASHSUM_REP = "'' AS configs_hashsum,"
RE_HIGH_WATERMARK_TIMESTAMP = r"'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}:\d{2}' AS TIMESTAMP"
HIGH_WATERMARK_VALUE_REP = "HIGH_WATERMARK_VALUE AS TIMESTAMP"
RE_CURRENT_TIMESTAMP = r"'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}\+\d{2}:\d{2}' AS TIMESTAMP"
CURRENT_TIMESTAMP_VALUE_REP = "CURRENT_TIMESTAMP_VALUE AS TIMESTAMP"


class TestJinjaTemplates:
    """ """

    @pytest.fixture(scope="session")
    def test_entities_collection(self, source_configs_path):
        """ """
        return lib.load_entities_config(configs_path=source_configs_path)

    @pytest.fixture(scope="session")
    def test_rules_collection(self, source_configs_path):
        """ """
        return lib.load_rules_config(configs_path=source_configs_path)

    @pytest.fixture(scope="session")
    def test_row_filters_collection(self, source_configs_path):
        """ """
        return lib.load_row_filters_config(configs_path=source_configs_path)

    @pytest.fixture(scope="session")
    def test_rule_bindings_collection_team_1(self, source_configs_path):
        """ """
        return lib.load_rule_bindings_config(
            source_configs_path / "rule_bindings" / "team-1-rule-bindings.yml"
        )

    @pytest.fixture(scope="session")
    def test_rule_bindings_collection_team_2(self, source_configs_path):
        """ """
        return lib.load_rule_bindings_config(
            source_configs_path / "rule_bindings" / "team-2-rule-bindings.yml"
        )

    @pytest.fixture(scope="session")
    def test_rule_bindings_collection_team_3(self, source_configs_path):
        """ """
        return lib.load_rule_bindings_config(
            source_configs_path / "rule_bindings" / "team-3-rule-bindings.yml"
        )

    @pytest.fixture(scope="session")
    def test_all_rule_bindings_collections(self, source_configs_path):
        """ """
        return lib.load_rule_bindings_config(configs_path=source_configs_path)

    def test_rules_class(self, test_rules_collection):
        for key, value in test_rules_collection.items():
            if value["rule_type"] == RuleType.CUSTOM_SQL_STATEMENT:
                continue
            if value["rule_type"] == RuleType.CUSTOM_SQL_EXPR and \
                value.get("params", {}).get("custom_sql_arguments", None):
                continue
            rule = DqRule.from_dict(rule_id=key, kwargs=value)
            assert key in rule.to_dict()
            value.update({"rule_sql_expr": rule.resolve_sql_expr()})
            if "params" not in value:
                value.update({"params": {}})
            if "dimension" not in value:
                value["dimension"] = None
            assert dict(rule.dict_values()) == dict(value)

    def test_filters_class(self, test_row_filters_collection):
        for key, value in test_row_filters_collection.items():
            dq_filter = DqRowFilter.from_dict(row_filter_id=key, kwargs=value)
            assert key in dq_filter.to_dict()
            assert dict(dq_filter.dict_values()) == dict(value)

    def test_resolve_time_filter_column(self, test_rule_bindings_collection_team_1, test_configs_cache):
        """ """
        logger.info(pformat(test_rule_bindings_collection_team_1))
        first_rule_binding_config = (
            test_rule_bindings_collection_team_1.values().__iter__().__next__()
        )
        rule_binding = DqRuleBinding.from_dict(
            rule_binding_id="dq_summary",
            kwargs=first_rule_binding_config,
        )
        entity = rule_binding.resolve_table_entity_config(configs_cache=test_configs_cache,)
        entity.resolve_column_config(rule_binding.incremental_time_filter_column_id)
        with pytest.raises(ValueError):
            entity.resolve_column_config("invalid_column")

    def test_rule_bindings_class(self, test_rule_bindings_collection_team_2):
        """

        Args:
          test_rule_bindings_collection_team_2:

        Returns:

        """
        for key, value in test_rule_bindings_collection_team_2.items():
            rule_binding = DqRuleBinding.from_dict(rule_binding_id=key, kwargs=value)
            assert key in rule_binding.to_dict()
            if "metadata" not in value:
                value.update({"metadata": {}})
            if "incremental_time_filter_column_id" not in value:
                value.update({"incremental_time_filter_column_id": None})
            value.update({'entity_uri': None})
            assert dict(rule_binding.dict_values()) == dict(value)

    def test_rule_bindings_class_resolve_configs(
        self,
        test_rule_bindings_collection_team_2,
        test_configs_cache,
        test_bigquery_client,
    ):
        for key, value in test_rule_bindings_collection_team_2.items():
            rule_binding = DqRuleBinding.from_dict(rule_binding_id=key, kwargs=value)
            rule_binding.resolve_table_entity_config(configs_cache=test_configs_cache)
            rule_binding.resolve_rule_config_list(configs_cache=test_configs_cache)
            rule_binding.resolve_row_filter_config(configs_cache=test_configs_cache)
            rule_binding.resolve_all_configs_to_dict(configs_cache=test_configs_cache, bigquery_client=test_bigquery_client)

    def test_render_run_dq_main_sql(
        self,
        test_rule_bindings_collection_team_2,
        test_configs_cache,
        test_resources,
        test_bigquery_client,
    ):
        """

        Args:
          test_rule_bindings_collection_team_2:
          test_entities_collection:
          test_rules_collection:
          test_row_filters_collection:

        Returns:

        """
        with open(test_resources / "test_render_run_dq_main_sql_expected.sql") as f:
            expected = f.read()
        rule_binding_id, rule_binding_configs = (
            test_rule_bindings_collection_team_2.items()
            .__iter__()
            .__next__()  # use first rule binding
        )
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
        output = re.sub(RE_NEWLINES, '\n', output).strip()
        output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
        expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
        assert output == expected

    def test_render_run_dq_main_sql_env_override(
        self,
        test_rule_bindings_collection_team_2,
        test_configs_cache,
        test_resources,
        test_bigquery_client,
    ):
        """

        Args:
          test_rule_bindings_collection_team_2:
          test_entities_collection:
          test_rules_collection:
          test_row_filters_collection:

        Returns:

        """
        with open(test_resources / "test_render_run_dq_main_sql_expected.sql") as f:
            expected = f.read()
        rule_binding_id, rule_binding_configs = (
            test_rule_bindings_collection_team_2.items()
            .__iter__()
            .__next__()  # use first rule binding
        )
        configs = lib.create_rule_binding_view_model(
            rule_binding_id=rule_binding_id,
            rule_binding_configs=rule_binding_configs,
            dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
            configs_cache=test_configs_cache,
            environment="TEST",
            debug=True,
            high_watermark_filter_exists=False,
            bigquery_client=test_bigquery_client,
        )
        output = configs.get("generated_sql_string_dict")\
                        .get(f"{rule_binding_id}_generated_sql_string")
        expected = expected.replace(
            "<your_gcp_project_id>.<your_bigquery_dataset_id>", "<your_gcp_project_id_2>.<your_bigquery_dataset_id_2>"
        )
        expected = expected.replace(
            "<your_bigquery_dataset_id>.__TABLES__", "<your_bigquery_dataset_id_2>.__TABLES__"
        )
        output = re.sub(RE_NEWLINES, '\n', output).strip()
        output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
        expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
        assert output == expected

    def test_render_run_dq_main_sql_high_watermark(
        self,
        test_rule_bindings_collection_team_1,
        test_configs_cache,
        test_resources,
        gcp_project_id,
        gcp_bq_dataset,
        test_bigquery_client,
    ):
        """

        Args:
          test_rule_bindings_collection_team_1:
          test_entities_collection:
          test_rules_collection:
          test_row_filters_collection:

        Returns:

        """
        with open(
            test_resources / "test_render_run_dq_main_sql_expected_high_watermark.sql",
        ) as f:
            expected = f.read()
        rule_binding_id, rule_binding_configs = (
            test_rule_bindings_collection_team_1.items()
            .__iter__()
            .__next__()  # use first rule binding
        )
        configs = lib.create_rule_binding_view_model(
            rule_binding_id=rule_binding_id,
            rule_binding_configs=rule_binding_configs,
            dq_summary_table_name=f"{gcp_project_id}.{gcp_bq_dataset}.dq_summary",
            configs_cache=test_configs_cache,
            environment="DEV",
            debug=True,
            dq_summary_table_exists=True,
            high_watermark_filter_exists=False,
            bigquery_client=test_bigquery_client,
        )
        output = configs.get("generated_sql_string_dict")\
                        .get(f"{rule_binding_id}_generated_sql_string")
        output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
        output = re.sub(RE_HIGH_WATERMARK_TIMESTAMP, HIGH_WATERMARK_VALUE_REP, output)
        output = re.sub(RE_CURRENT_TIMESTAMP, CURRENT_TIMESTAMP_VALUE_REP, output)
        output = re.sub(RE_NEWLINES, '\n', output).strip()
        expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
        assert output == expected

    def test_render_run_dq_main_sql_custom_sql_statement(
        self,
        test_rule_bindings_collection_team_3,
        test_configs_cache,
        test_resources,
        test_bigquery_client,
    ):
        """

        Args:
          test_rule_bindings_collection_team_3:
          test_entities_collection:
          test_rules_collection:
          test_row_filters_collection:

        Returns:

        """
        with open(
            test_resources / "test_render_run_dq_main_sql_expected_custom_sql_statement.sql",
        ) as f:
            expected = f.read()
        rule_binding_id, rule_binding_configs = (
            test_rule_bindings_collection_team_3.items()
            .__iter__()
            .__next__()  # use first rule binding
        )
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
        output = re.sub(RE_CONFIGS_HASHSUM, CONFIGS_HASHSUM_REP, output)
        output = re.sub(RE_NEWLINES, '\n', output).strip()
        expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected)).strip()
        assert output == expected

    def test_prepare_configs_from_rule_binding(
        self, test_rule_bindings_collection_team_2, test_configs_cache, test_resources, test_bigquery_client,
    ):
        """ """
        rule_binding_id, rule_binding_configs = (
            test_rule_bindings_collection_team_2.items()
            .__iter__()
            .__next__()  # use first rule binding
        )
        env = "DEV"
        metadata = {"channel": "two"}
        configs = lib.prepare_configs_from_rule_binding_id(
            rule_binding_id=rule_binding_id,
            rule_binding_configs=rule_binding_configs,
            dq_summary_table_name="<your_gcp_project_id>.<your_bigquery_dataset_id>.dq_summary",
            environment=env,
            metadata=metadata,
            configs_cache=test_configs_cache,
            bigquery_client=test_bigquery_client,
        )
        logger.info(pformat(json.dumps(configs["configs"])))
        with open(test_resources / "expected_configs.json") as f:
            expected_configs = json.loads(f.read())
        assert configs["configs"] == dict(expected_configs)
        metadata.update(rule_binding_configs["metadata"])
        assert configs["metadata"] == dict(metadata)
        assert configs["environment"] == env

    def test_create_entity_summary_model(self, test_resources):
        entity_target_rule_binding_configs = {
            "rule_binding_ids_list": [
                "rule_binding_id_1",
                "rule_binding_id_2",
            ]
        }

        entity_summary_model = lib.create_entity_summary_model(
            entity_table_id="entity_table_id",
            entity_target_rule_binding_configs=entity_target_rule_binding_configs,
            gcp_project_id="gcp_project_id",
            gcp_bq_dataset_id="gcp_bq_dataset_id",
            debug=True,)
        with open(test_resources / "expected_entity_summary_model.sql") as f:
            expected_entity_summary_model = f.read()
            expected = utils.strip_margin(re.sub(RE_NEWLINES, '\n', expected_entity_summary_model)).strip()
        assert expected == utils.strip_margin(re.sub(RE_NEWLINES, '\n', entity_summary_model)).strip()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, '-vv', '-rP', '-n', 'auto']))
