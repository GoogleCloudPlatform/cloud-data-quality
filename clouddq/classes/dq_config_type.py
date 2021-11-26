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

from enum import Enum
from enum import unique

from clouddq.classes.dq_entity import DqEntity
from clouddq.classes.dq_row_filter import DqRowFilter
from clouddq.classes.dq_rule import DqRule
from clouddq.classes.dq_rule_binding import DqRuleBinding


@unique
class DqConfigType(str, Enum):
    """ """

    RULES = "rules"
    RULE_BINDINGS = "rule_bindings"
    ROW_FILTERS = "row_filters"
    ENTITIES = "entities"
    RULE_DIMENSIONS = "rule_dimensions"

    def to_class(
        self: DqConfigType,
    ) -> type[DqRule] | type[DqRuleBinding] | type[DqRowFilter] | type[DqEntity]:
        if self == DqConfigType.RULES:
            return DqRule
        elif self == DqConfigType.RULE_BINDINGS:
            return DqRuleBinding
        elif self == DqConfigType.ROW_FILTERS:
            return DqRowFilter
        elif self == DqConfigType.ENTITIES:
            return DqEntity
        else:
            raise NotImplementedError(f"DQ Config Type: {self} not implemented.")
