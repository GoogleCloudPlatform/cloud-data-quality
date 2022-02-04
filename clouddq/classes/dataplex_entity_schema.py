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
from clouddq.classes.dataplex_entity_schema_partition_fields import DataplexEntityPartitionSchemaField
from clouddq.utils import assert_not_none_or_empty


@dataclass
class DataplexEntitySchema:
    """ """

    fields: list[DataplexEntitySchemaField]
    partitionFields: list[DataplexEntityPartitionSchemaField]
    partitionStyle: str

    @classmethod
    def from_dict(
        cls: DataplexEntitySchema, entity_id: str, kwargs: dict
    ) -> DataplexEntitySchema:

        fields: list[dict] = []
        fields_list = kwargs.get("fields")
        if fields_list:
            for obj in fields_list:
                field = DataplexEntitySchemaField.from_dict(
                    entity_id=entity_id, kwargs=obj
                )
                assert_not_none_or_empty(
                    value=field,
                    error_msg=f"Entity {entity_id}: must define non-empty value: 'fields'.",
                )
                fields.append(field.to_dict())
        else:
            fields = None
        partition_fields: list[dict] = []
        if kwargs.get("partitionFields"):
            partition_fields_list = kwargs.get("partitionFields")
            for obj in partition_fields_list:
                partition_field = DataplexEntityPartitionSchemaField.from_dict(
                    entity_id=entity_id, kwargs=obj
                )
                assert_not_none_or_empty(
                    value=partition_field,
                    error_msg=f"Entity {entity_id}: must define non-empty value: "
                    f"'partitionFields'.",
                )
                partition_fields.append(partition_field.to_dict())
        else:
            partition_fields = None

        if kwargs.get("partitionStyle"):
            partition_style = kwargs.get("partitionStyle", None)
            assert_not_none_or_empty(
                value=partition_style,
                error_msg=f"Entity {entity_id}: must define non-empty value: 'partition_style'.",
            )
        else:
            partition_style = None

        return DataplexEntitySchema(
            fields=fields,
            partitionFields=partition_fields,
            partitionStyle=partition_style,
        )

    def to_dict(self: DataplexEntitySchema) -> dict:
        """
        Args:
          self: DataplexEntitySchema:

        Returns:

        """

        output = {
            "fields": self.fields,
            "partitionFields": self.partitionFields,
            "partitionStyle": self.partitionStyle,
        }

        return dict(output)
