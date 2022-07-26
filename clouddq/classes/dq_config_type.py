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
from clouddq.classes.dq_reference_columns import DqReferenceColumns
from clouddq.classes.dq_row_filter import DqRowFilter
from clouddq.classes.dq_rule import DqRule
from clouddq.classes.dq_rule_binding import DqRuleBinding
from clouddq.classes.dq_rule_dimensions import DqRuleDimensions
from clouddq.classes.metadata_registry_defaults import MetadataRegistryDefaults


@unique
class DqConfigType(str, Enum):
    """ """

    RULES = "rules"
    RULE_BINDINGS = "rule_bindings"
    RULE_DIMENSIONS = "rule_dimensions"
    ROW_FILTERS = "row_filters"
    REFERENCE_COLUMNS = "reference_columns"
    ENTITIES = "entities"
    METADATA_REGISTRY_DEFAULTS = "metadata_registry_defaults"

    def is_required(
        self: DqConfigType,
    ) -> bool:
        if self == DqConfigType.RULES:
            return True
        elif self == DqConfigType.RULE_BINDINGS:
            return True
        elif self == DqConfigType.RULE_DIMENSIONS:
            return False
        elif self == DqConfigType.ROW_FILTERS:
            return True
        elif self == DqConfigType.REFERENCE_COLUMNS:
            return False
        elif self == DqConfigType.ENTITIES:
            return False
        elif self == DqConfigType.METADATA_REGISTRY_DEFAULTS:
            return False
        else:
            raise NotImplementedError(f"DQ Config Type: {self} not implemented.")

    def to_class(
        self: DqConfigType,
    ) -> type[DqRule] | type[DqRuleBinding] | type[DqRuleDimensions] | type[
        DqRowFilter
    ] | type[DqEntity] | type[MetadataRegistryDefaults]:
        if self == DqConfigType.RULES:
            return DqRule
        elif self == DqConfigType.RULE_BINDINGS:
            return DqRuleBinding
        elif self == DqConfigType.RULE_DIMENSIONS:
            return DqRuleDimensions
        elif self == DqConfigType.ROW_FILTERS:
            return DqRowFilter
        elif self == DqConfigType.REFERENCE_COLUMNS:
            return DqReferenceColumns
        elif self == DqConfigType.ENTITIES:
            return DqEntity
        elif self == DqConfigType.METADATA_REGISTRY_DEFAULTS:
            return MetadataRegistryDefaults
        else:
            raise NotImplementedError(f"DQ Config Type: {self} not implemented.")
