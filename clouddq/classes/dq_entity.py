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

from clouddq.classes.dq_entity_column import DqEntityColumn
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import get_from_dict_and_assert


@dataclass
class DqEntity:
    """ """

    entity_id: str
    source_database: str
    table_name: str
    database_name: str
    instance_name: str
    columns: dict[str, DqEntityColumn]
    environment_override: dict

    def resolve_column_config(self: DqEntity, column_id: str) -> DqEntityColumn:
        """

        Args:
          self: DqRuleBinding:
          column_id: str:

        Returns:

        """

        dq_column_config = self.columns.get(column_id, None)
        assert_not_none_or_empty(
            dq_column_config,
            f"Column ID: {column_id} not found in Entity Config: {self.entity_id}.",
        )
        return dq_column_config

    @classmethod
    def from_dict(cls: DqEntity, entity_id: str, kwargs: dict) -> DqEntity:
        """

        Args:
          cls: DqEntity:
          entity_id: str:
          kwargs: typing.Dict:

        Returns:

        """

        source_database = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="source_database"
        )
        table_name = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="table_name"
        )
        database_name = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="database_name"
        )
        instance_name = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="instance_name"
        )
        columns_dict = get_from_dict_and_assert(
            config_id=entity_id, kwargs=kwargs, key="columns"
        )
        columns: dict[str, DqEntityColumn] = dict()
        for column_id, column_config in columns_dict.items():
            column = DqEntityColumn.from_dict(
                entity_column_id=column_id,
                kwargs=column_config,
                entity_source_database=source_database,
            )
            columns[column_id] = column
        # todo: validate environment override
        environment_override: dict = kwargs.get("environment_override", dict())
        return DqEntity(
            entity_id=str(entity_id),
            source_database=source_database,
            table_name=table_name,
            database_name=database_name,
            instance_name=instance_name,
            columns=columns,
            environment_override=environment_override,
        )

    def to_dict(self: DqEntity) -> dict:
        """

        Args:
          self: DqEntity:

        Returns:

        """
        columns = {
            column_id: column_config.dict_values()
            for column_id, column_config in self.columns.items()
        }
        output = {
            "source_database": self.source_database,
            "table_name": self.table_name,
            "database_name": self.database_name,
            "instance_name": self.instance_name,
            "columns": columns,
        }
        if self.environment_override:
            output["environment_override"] = self.environment_override
        return dict({f"{self.entity_id}": output})

    def dict_values(self: DqEntity) -> dict:
        """

        Args:
          self: DqEntity:

        Returns:

        """

        return dict(self.to_dict().get(self.entity_id))
