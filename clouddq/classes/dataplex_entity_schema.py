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

from clouddq.classes.dataplex_entity_schema_field import DataplexEntitySchemaField
from clouddq.utils import assert_not_none_or_empty


@dataclass
class DataplexEntitySchema:
    """ """

    fields: list[DataplexEntitySchemaField]

    @classmethod
    def from_dict(
        cls: DataplexEntitySchema, entity_id: str, kwargs: dict
    ) -> DataplexEntitySchema:

        fields = kwargs.get("fields", None)
        assert_not_none_or_empty(
            value=fields,
            error_msg=f"Entity {entity_id}: must define non-empty value: 'fields'.",
        )

        return DataplexEntitySchema(
            fields=fields,
        )

    def to_dict(self: DataplexEntitySchema) -> dict:
        """
        Args:
          self: DataplexEntitySchemaField:

        Returns:

        """

        output = {
            "fields": self.fields,
        }

        return dict(output)
