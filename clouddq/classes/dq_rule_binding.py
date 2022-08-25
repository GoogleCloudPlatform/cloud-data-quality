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

"""todo: add classes docstring."""
from __future__ import annotations

from dataclasses import dataclass
from pprint import pformat
from string import Template

import logging

from clouddq.classes.dq_entity import DqEntity
from clouddq.classes.dq_entity_column import DqEntityColumn
from clouddq.classes.dq_entity_uri import EntityUri
from clouddq.classes.dq_reference_columns import DqReferenceColumns
from clouddq.classes.dq_row_filter import DqRowFilter
from clouddq.classes.dq_rule import DqRule
from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import get_from_dict_and_assert
from clouddq.utils import get_keys_from_dict_and_assert_oneof

import clouddq.classes.dq_configs_cache as dq_configs_cache


logger = logging.getLogger(__name__)


@dataclass
class DqRuleBinding:
    """ """

    rule_binding_id: str
    entity_id: str | None
    entity_uri: EntityUri | None
    column_id: str
    row_filter_id: str
    incremental_time_filter_column_id: str | None
    rule_ids: list
    reference_columns_id: str | None
    metadata: dict | None

    @classmethod
    def from_dict(
        cls: DqRuleBinding,
        rule_binding_id: str,
        kwargs: dict,
        default_configs: dict | None = None,
        validate_uri: bool = True,
    ) -> DqRuleBinding:
        """

        Args:
          cls: DqRuleBinding:
          rule_binding_id: typing.Union[str, str]:
          kwargs: typing.Dict:

        Returns:

        """
        entity_config: dict = get_keys_from_dict_and_assert_oneof(
            config_id=rule_binding_id,
            kwargs=kwargs,
            keys=["entity_uri", "entity_id"],
        )
        if "entity_id" in entity_config:
            entity_id = entity_config["entity_id"]
            entity_uri = None
        if "entity_uri" in entity_config:
            parsed_entity_uri = EntityUri.from_uri(
                entity_config["entity_uri"],
                default_configs=default_configs,
                validate_uri=validate_uri,
            )
            entity_id = parsed_entity_uri.get_entity_id()
            entity_uri = parsed_entity_uri
        if entity_id:
            entity_id.upper()
        column_id: str = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="column_id",
        )
        if column_id:
            column_id.upper()
        row_filter_id: str = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="row_filter_id",
        )
        if row_filter_id:
            row_filter_id.upper()
        rule_ids: list[str] = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="rule_ids",
            assertion=lambda x: type(x) == list,
            error_msg=f"Rule Binding ID: '{rule_binding_id}' must have defined value "
            f"'rule_ids' of type 'list'.",
        )
        reference_columns_id: str = kwargs.get("reference_columns_id", None)
        if reference_columns_id:
            reference_columns_id.upper()
        incremental_time_filter_column_id: str | None = kwargs.get(
            "incremental_time_filter_column_id", None
        )
        if incremental_time_filter_column_id:
            incremental_time_filter_column_id.upper()
        metadata: dict | None = kwargs.get("metadata", dict())
        if type(metadata) != dict:
            raise ValueError(
                f"Rule Binding ID: '{rule_binding_id}' has invalid "
                f"metadata field with type {type(metadata)} and values: {metadata}\n"
                "'metadata' must be of type dictionary."
            )
        return DqRuleBinding(
            rule_binding_id=str(rule_binding_id).upper(),
            entity_id=entity_id,
            entity_uri=entity_uri,
            column_id=column_id,
            row_filter_id=row_filter_id,
            incremental_time_filter_column_id=incremental_time_filter_column_id,
            rule_ids=rule_ids,
            reference_columns_id=reference_columns_id,
            metadata=metadata,
        )

    def to_dict(self: DqRuleBinding) -> dict:
        """

        Args:
          self: DqRuleBinding:

        Returns:

        """
        if self.entity_uri:
            entity_uri = self.entity_uri.to_dict()
        else:
            entity_uri = None

        return dict(
            {
                f"{self.rule_binding_id}": {
                    "entity_id": self.entity_id,
                    "entity_uri": entity_uri,
                    "column_id": self.column_id,
                    "row_filter_id": self.row_filter_id,
                    "incremental_time_filter_column_id": self.incremental_time_filter_column_id,  # noqa: E501
                    "rule_ids": self.rule_ids,
                    "reference_columns_id": self.reference_columns_id,
                    "metadata": self.metadata,
                }
            }
        )

    def dict_values(self: DqRuleBinding) -> dict:
        """

        Args:
          self: DqRuleBinding:

        Returns:

        """

        return dict(self.to_dict().get(self.rule_binding_id))

    def resolve_table_entity_config(
        self: DqRuleBinding, configs_cache: dq_configs_cache.DqConfigsCache
    ) -> DqEntity:
        """

        Args:
          self: DqRuleBinding:
          entities_collection: typing.Dict:

        Returns:

        """
        if self.entity_uri:
            logger.debug(
                f"Resolving entity uri from configs cache:\n{pformat(self.entity_uri.to_dict())}"
            )
            table_entity: DqEntity = configs_cache.get_table_entity_id(
                self.entity_uri.get_db_primary_key().upper()
            )
        elif self.entity_id:
            table_entity: DqEntity = configs_cache.get_table_entity_id(
                self.entity_id.upper()
            )
        else:
            raise ValueError(
                f"Rule Binding ID: {self.rule_binding_id} must define "
                "either 'entity_id' or 'entity_uri'."
            )
        return table_entity

    def resolve_rule_sql_expr(self: DqRuleBinding, rule: DqRule) -> None:
        try:
            rule.resolve_sql_expr()
        except Exception as e:
            raise ValueError(
                f"Failed to resolve rule_id '{rule.rule_id}' in "
                f"rule_binding_id '{self.rule_binding_id}' "
                f"with error:\n{e}"
            )

    def resolve_rule_config_list(
        self: DqRuleBinding,
        configs_cache: dq_configs_cache.DqConfigsCache,
    ) -> list[DqRule]:
        """

        Args:
          self: DqRuleBinding:
          rules_collection: typing.Dict:

        Returns:

        """

        resolved_rule_config_list = []
        for rule in self.rule_ids:
            if type(rule) == dict:
                if len(rule) > 1:
                    raise ValueError(
                        f"Rule Binding {self.rule_binding_id} has "
                        f"invalid configs in rule_ids. "
                        f"Each nested rule_id objects cannot "
                        f"have more than one rule_id. "
                        f"Current value: \n {rule}"
                    )
                else:
                    rule_id = next(iter(rule))
                    arguments = rule[rule_id]
            else:
                rule_id = rule
                arguments = None
            rule_config = configs_cache.get_rule_id(rule_id.upper())
            rule_config.update_rule_binding_arguments(arguments)
            self.resolve_rule_sql_expr(rule_config)
            resolved_rule_config_list.append(rule_config)
        assert_not_none_or_empty(
            resolved_rule_config_list,
            "Rule Binding must have non-empty rule_ids list.",
        )
        return resolved_rule_config_list

    def resolve_row_filter_config(
        self: DqRuleBinding,
        configs_cache: dq_configs_cache.DqConfigsCache,
    ) -> DqRowFilter:
        row_filter = configs_cache.get_row_filter_id(self.row_filter_id.upper())
        return row_filter

    def resolve_reference_columns_config(
        self: DqRuleBinding,
        configs_cache: dq_configs_cache.DqConfigsCache,
    ) -> DqReferenceColumns:
        reference_columns = configs_cache.get_reference_columns_id(
            self.reference_columns_id.upper()
        )
        return reference_columns

    def resolve_all_configs_to_dict(
        self: DqRuleBinding,
        configs_cache: dq_configs_cache.DqConfigsCache,
        bigquery_client: BigQueryClient,
    ) -> dict:
        """

        Args:
          self: DqRuleBinding:
          entities_collection: typing.Dict:
          rules_collection: typing.Dict:
          row_filters_collection: typing.Dict:

        Returns:

        """
        try:
            # Resolve table configs
            table_entity: DqEntity = self.resolve_table_entity_config(configs_cache)
            # Resolve column configs
            column_configs: DqEntityColumn = table_entity.resolve_column_config(
                self.column_id.upper()
            )
            incremental_time_filter_column = None
            if self.incremental_time_filter_column_id:
                incremental_time_filter_column_config: DqEntityColumn = (
                    table_entity.resolve_column_config(
                        self.incremental_time_filter_column_id.upper()
                    )
                )
                incremental_time_filter_column_type: str = (
                    incremental_time_filter_column_config.get_column_type_value()
                )
                if incremental_time_filter_column_type not in ("TIMESTAMP", "DATETIME"):
                    raise ValueError(
                        f"incremental_time_filter_column_id: "
                        f"{self.incremental_time_filter_column_id} "
                        f"must have type TIMESTAMP or DATETIME.\n"
                        f"Current type: {incremental_time_filter_column_type}."
                    )
                incremental_time_filter_column = dict(
                    incremental_time_filter_column_config.dict_values()
                ).get("name")
            # Resolve rules configs
            rule_configs_dict = dict()
            for rule in self.resolve_rule_config_list(configs_cache):
                for rule_id, rule_config in rule.to_dict().items():
                    rule_id = rule_id.upper()
                    rule_sql_expr = Template(
                        rule_config["rule_sql_expr"]
                    ).safe_substitute(
                        column=column_configs.column_name
                        if column_configs.column_name != "data"
                        else f"data.{column_configs.column_name}"
                    )
                    rule_config["rule_sql_expr"] = rule_sql_expr
                    rule_configs_dict[rule_id] = rule_config
            # Resolve filter configs
            row_filter_config = self.resolve_row_filter_config(configs_cache)
            # resolve reference columns config
            if self.reference_columns_id:
                include_reference_columns = self.resolve_reference_columns_config(
                    configs_cache
                ).include_reference_columns
                if include_reference_columns[0] == "*":
                    column_names = bigquery_client.get_table_columns(
                        table=table_entity.get_table_name(),
                        project_id=table_entity.instance_name,
                    )
                    include_reference_columns = sorted(column_names)
            else:
                include_reference_columns = []

            return dict(
                {
                    "rule_binding_id": self.rule_binding_id,
                    "entity_id": self.entity_id,
                    "entity_configs": dict(table_entity.dict_values()),
                    "column_id": self.column_id,
                    "reference_columns_id": self.reference_columns_id,
                    "include_reference_columns": include_reference_columns,
                    "column_configs": dict(column_configs.dict_values()),
                    "rule_ids": list(self.rule_ids),
                    "rule_configs_dict": rule_configs_dict,
                    "row_filter_id": self.row_filter_id,
                    "row_filter_configs": dict(row_filter_config.dict_values()),
                    "incremental_time_filter_column": incremental_time_filter_column,
                    "metadata": self.metadata,
                }
            )
        except Exception as error:
            raise ValueError(
                f"Failed to resolve Rule Binding ID '{self.rule_binding_id}' with error:\n{error}"
            )
