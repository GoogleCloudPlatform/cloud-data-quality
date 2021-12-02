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

from clouddq.classes.rule_type import RuleType

import clouddq.classes


@dataclass
class DqRule:
    """ """

    rule_id: str
    rule_type: RuleType
    dimension: str | None = None
    params: dict | None = None

    @classmethod
    def update_config(cls: DqRule, config_current: dict, config_new: dict) -> dict:
        return clouddq.classes.update_config(config_current, config_new)

    @classmethod
    def validate(cls: DqRule, config: dict, rule_dims: list) -> None:
        if "dimension" in config and not config["dimension"] in rule_dims:
            raise ValueError(
                f"Rule is invalid because dimension {config['dimension']}"
                " is not an allowed value: {rule_dims}"
            )

    @classmethod
    def from_dict(cls: DqRule, rule_id: str, kwargs: dict) -> DqRule:
        """

        Args:
          cls: DqRule:
          rule_id: str:
          kwargs: typing.Dict:

        Returns:

        """

        rule_type: RuleType = RuleType(kwargs.get("rule_type", ""))
        params: dict = kwargs.get("params", dict())
        dim: str = kwargs.get("dimension")
        return DqRule(
            rule_id=str(rule_id), rule_type=rule_type, dimension=dim, params=params
        )

    def to_dict(self: DqRule) -> dict:
        """

        Args:
          self: DqRule:

        Returns:

        """

        d = {
            f"{self.rule_id}": {
                "rule_type": self.rule_type.name,
                "params": self.params,
                "rule_sql_expr": self.resolve_sql_expr(),
            }
        }

        if self.dimension:
            d[self.rule_id]["dimension"] = self.dimension

        return d

    def dict_values(self: DqRule) -> dict:
        """

        Args:
          self: DqRule:

        Returns:

        """

        return dict(self.to_dict().get(self.rule_id))

    def resolve_sql_expr(self: DqRule) -> str:
        return self.rule_type.to_sql(self.params).safe_substitute()

    def update_rule_binding_arguments(self, arguments: dict) -> None:
        params = {"rule_binding_arguments": arguments}
        if not self.params:
            self.params = params
        elif type(self.params) == dict:
            self.params.update(params)
        else:
            raise ValueError(
                f"DqRule ID: {self.rule_id} has invalid 'params' field:\n {self.params}"
            )
