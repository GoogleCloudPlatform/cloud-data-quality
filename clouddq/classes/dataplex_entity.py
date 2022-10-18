# Copyright 2022 Google LLC
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

from clouddq.classes.dataplex_entity_schema import DataplexEntitySchema
from clouddq.utils import assert_not_none_or_empty


@dataclass
class DataplexEntity:
    """ """

    name: str
    createTime: str
    updateTime: str
    id: str
    type: str
    asset: str
    dataPath: str
    system: str
    format: dict
    schema: DataplexEntitySchema

    @property
    def project_id(self):
        return self.name.split("/")[1]

    @property
    def location(self):
        return self.name.split("/")[3]

    @property
    def lake(self):
        return self.name.split("/")[5]

    @property
    def zone(self):
        return self.name.split("/")[7]

    def get_db_primary_key(self):
        return (
            f"projects/{self.project_id}/locations/{self.location}/"
            + f"lakes/{self.lake}/zones/{self.zone}/entities/{self.id}"  # noqa: W503
        )

    @classmethod
    def from_dict(cls: DataplexEntity, entity_id: str, kwargs: dict) -> DataplexEntity:
        """

        Args:
          cls: DataplexEntity:
          kwargs: typing.Dict:

        Returns:

        """

        try:
            entity_name = kwargs.get("name", None)
            assert_not_none_or_empty(
                value=entity_name,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'name'.",
            )

            create_time = kwargs.get("createTime", None)
            assert_not_none_or_empty(
                value=create_time,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'create_time'.",
            )

            update_time = kwargs.get("updateTime", None)
            assert_not_none_or_empty(
                value=update_time,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'update_time'.",
            )

            entity_id = kwargs.get("id", None)
            assert_not_none_or_empty(
                value=entity_id,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'id'.",
            )

            entity_type = kwargs.get("type", None)
            assert_not_none_or_empty(
                value=entity_type,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'entity_type'.",
            )

            asset = kwargs.get("asset", None)
            assert_not_none_or_empty(
                value=asset,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'asset'.",
            )

            data_path = kwargs.get("dataPath", None)
            assert_not_none_or_empty(
                value=data_path,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'data_path'.",
            )

            system = kwargs.get("system", None)
            assert_not_none_or_empty(
                value=system,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'system'.",
            )

            entity_format = kwargs.get("format", None)
            if system != "BIGQUERY":
                assert_not_none_or_empty(
                    value=entity_format,
                    error_msg=f"Entity {entity_id} must define non-empty value for field: 'format'.",
                )

            schema_dict = kwargs.get("schema", None)
            assert_not_none_or_empty(
                value=schema_dict,
                error_msg=f"Entity {entity_id} must define non-empty value for field: 'schema'.",
            )

            schema = DataplexEntitySchema.from_dict(
                entity_id=entity_id, kwargs=schema_dict
            )

            return DataplexEntity(
                name=entity_name,
                createTime=create_time,
                updateTime=update_time,
                id=entity_id,
                type=entity_type,
                asset=asset,
                dataPath=data_path,
                system=system,
                format=entity_format,
                schema=schema,
            )
        except ValueError as e:
            raise ValueError(
                f"Failed to parse Dataplex Entity response with error:\n\t{e}\n"
                f"Input Dataplex Entity response:\n\t{format(kwargs)}"
            )

    def to_dict(self: DataplexEntity) -> dict:
        """
        Serialize dataplex entity to dictionary
        Args:
          self: DataplexEntity:

        Returns:

        """

        output = {
            "name": self.name,
            "createTime": self.createTime,
            "updateTime": self.updateTime,
            "id": self.id,
            "type": self.type,
            "asset": self.asset,
            "dataPath": self.dataPath,
            "system": self.system,
            "format": self.format,
            "schema": self.schema.to_dict(),
            "project_id": self.project_id,
            "location": self.location,
            "lake": self.lake,
            "zone": self.zone,
            "db_primary_key": self.get_db_primary_key(),
        }

        return dict(output)
