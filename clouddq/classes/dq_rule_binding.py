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
from string import Template

from clouddq.classes.dq_entity import DqEntity
from clouddq.classes.dq_entity_column import DqEntityColumn
from clouddq.classes.dq_row_filter import DqRowFilter
from clouddq.classes.dq_rule import DqRule
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import get_from_dict_and_assert


@dataclass
class DqRuleBinding:
    """ """

    rule_binding_id: str
    entity_id: str
    column_id: str
    row_filter_id: str
    incremental_time_filter_column_id: str | None
    rule_ids: list
    metadata: dict | None

    @classmethod
    def from_dict(
        cls: DqRuleBinding,
        rule_binding_id: str,
        kwargs: dict,
    ) -> DqRuleBinding:
        """

        Args:
          cls: DqRuleBinding:
          rule_binding_id: typing.Union[str, str]:
          kwargs: typing.Dict:

        Returns:

        """
        entity_id: str = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="entity_id",
        )
        column_id: str = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="column_id",
        )
        row_filter_id: str = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="row_filter_id",
        )
        rule_ids: list[str] = get_from_dict_and_assert(
            config_id=rule_binding_id,
            kwargs=kwargs,
            key="rule_ids",
            assertion=lambda x: type(x) == list,
            error_msg=f"Rule Binding ID: '{rule_binding_id}' must have defined value "
            f"'rule_ids' of type 'list'.",
        )
        incremental_time_filter_column_id: str | None = kwargs.get(
            "incremental_time_filter_column_id", None
        )
        metadata: dict | None = kwargs.get("metadata", dict())
        return DqRuleBinding(
            rule_binding_id=str(rule_binding_id),
            entity_id=entity_id,
            column_id=column_id,
            row_filter_id=row_filter_id,
            incremental_time_filter_column_id=incremental_time_filter_column_id,
            rule_ids=rule_ids,
            metadata=metadata,
        )

    def to_dict(self: DqRuleBinding) -> dict:
        """

        Args:
          self: DqRuleBinding:

        Returns:

        """

        return dict(
            {
                f"{self.rule_binding_id}": {
                    "entity_id": self.entity_id,
                    "column_id": self.column_id,
                    "row_filter_id": self.row_filter_id,
                    "incremental_time_filter_column_id": self.incremental_time_filter_column_id,  # noqa: E501
                    "rule_ids": self.rule_ids,
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
        self: DqRuleBinding, entities_collection: dict
    ) -> DqEntity:
        """

        Args:
          self: DqRuleBinding:
          entities_collection: typing.Dict:

        Returns:

        """

        table_entity_dict = entities_collection.get(self.entity_id, None)
        assert_not_none_or_empty(
            table_entity_dict,
            f"Table Entity_ID: {self.entity_id} not found in Entities Config.",
        )
        table_entity = DqEntity.from_dict(self.entity_id, table_entity_dict)
        return table_entity

    def resolve_rule_config_list(
        self: DqRuleBinding, rules_collection: dict
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
                        f"Rach nested rule_id objects cannot "
                        f"have more than one rule_id. "
                        f"Current value: \n {rule}"
                    )
                else:
                    rule_id = next(iter(rule))
                    arguments = rule[rule_id]
            else:
                rule_id = rule
                arguments = None
            rule_config_dict = rules_collection.get(rule_id, None)
            assert_not_none_or_empty(
                rule_config_dict, f"Rule_ID: {rule_id} not found in Rules Config."
            )
            if arguments:
                rule_config_dict["params"]["rule_binding_arguments"] = arguments
            rule_config = DqRule.from_dict(rule_id, rule_config_dict)
            resolved_rule_config_list.append(rule_config)
        assert_not_none_or_empty(
            resolved_rule_config_list,
            "Rule Binding must have non-empty rule_config_list.",
        )
        return resolved_rule_config_list

    def resolve_row_filter_config(
        self: DqRuleBinding, row_filters_collection: dict
    ) -> DqRowFilter:
        """

        Args:
          self: DqRuleBinding:
          row_filters_collection: typing.Dict:

        Returns:

        """

        row_filter_config_dict = row_filters_collection.get(self.row_filter_id, None)
        assert_not_none_or_empty(
            row_filter_config_dict,
            f"Row Filter ID: {self.row_filter_id} not found in Filters Config.",
        )
        row_filter_config = DqRowFilter.from_dict(
            self.row_filter_id, row_filter_config_dict
        )
        return row_filter_config

    def resolve_all_configs_to_dict(
        self: DqRuleBinding,
        entities_collection: dict,
        rules_collection: dict,
        row_filters_collection: dict,
    ) -> dict:
        """

        Args:
          self: DqRuleBinding:
          entities_collection: typing.Dict:
          rules_collection: typing.Dict:
          row_filters_collection: typing.Dict:

        Returns:

        """

        # Resolve table configs
        table_entity: DqEntity = self.resolve_table_entity_config(entities_collection)
        # Resolve column configs
        column_configs: DqEntityColumn = table_entity.resolve_column_config(
            self.column_id
        )
        incremental_time_filter_column = None
        if self.incremental_time_filter_column_id:
            incremental_time_filter_column_config: DqEntityColumn = (
                table_entity.resolve_column_config(
                    self.incremental_time_filter_column_id
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
        for rule in self.resolve_rule_config_list(rules_collection):
            for rule_id, rule_config in rule.to_dict().items():
                rule_sql_expr = Template(rule_config["rule_sql_expr"]).safe_substitute(
                    column=column_configs.column_name
                )
                rule_config["rule_sql_expr"] = rule_sql_expr
                rule_configs_dict[rule_id] = rule_config
        # Resolve filter configs
        row_filter_config = self.resolve_row_filter_config(row_filters_collection)
        return dict(
            {
                "rule_binding_id": self.rule_binding_id,
                "entity_id": self.entity_id,
                "entity_configs": dict(table_entity.dict_values()),
                "column_id": self.column_id,
                "column_configs": dict(column_configs.dict_values()),
                "rule_ids": list(self.rule_ids),
                "rule_configs_dict": rule_configs_dict,
                "row_filter_id": self.row_filter_id,
                "row_filter_configs": dict(row_filter_config.dict_values()),
                "incremental_time_filter_column": incremental_time_filter_column,
                "metadata": self.metadata,
            }
        )
