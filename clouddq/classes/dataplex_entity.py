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

from dataclasses import dataclass, asdict
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

    @classmethod
    def from_dict(cls: DataplexEntity, kwargs: dict) -> DataplexEntity:
        """

        Args:
          cls: DataplexEntity:
          kwargs: typing.Dict:

        Returns:

        """

        entity_name = kwargs.get("name")
        assert_not_none_or_empty(value=entity_name,
                error_msg=f"Name: must define non-empty value: 'name'.")

        create_time = kwargs.get("createTime")
        assert_not_none_or_empty(value=create_time,
                error_msg=f"Create Time: must define non-empty value: 'create_time'.")

        update_time = kwargs.get("updateTime")
        assert_not_none_or_empty(value=update_time,
                error_msg=f"Update Time: must define non-empty value: 'update_time'.")

        entity_id = kwargs.get("id")
        assert_not_none_or_empty(value=entity_id,
                error_msg=f"Id: must define non-empty value: 'id'.")

        entity_type = kwargs.get("type")
        assert_not_none_or_empty(value=entity_type,
                error_msg=f"Entity type: must define non-empty value: 'entity_type'.")

        asset = kwargs.get("asset")
        assert_not_none_or_empty(value=asset,
                error_msg=f"Asset: must define non-empty value: 'asset'.")

        data_path = kwargs.get("dataPath")
        assert_not_none_or_empty(value=data_path,
                error_msg=f"Data Path: must define non-empty value: 'data_path'.")

        system = kwargs.get("system")
        assert_not_none_or_empty(value=system,
                error_msg=f"System: must define non-empty value: 'system'.")

        entity_format = kwargs.get("format")
        assert_not_none_or_empty(value=entity_format,
                error_msg=f"Format: must define non-empty value: 'format'.")

        schema_dict = kwargs.get("schema")
        assert_not_none_or_empty(value=schema_dict,
                error_msg=f"Schema: must define non-empty value: 'schema'.")

        schema = DataplexEntitySchema.from_dict(kwargs=schema_dict)


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

    def to_dict(self: DataplexEntity) -> dict:
        """
        Serialize dataplex entity to dictionary
        Args:
          self: DataplexEntity:

        Returns:

        """

        return asdict(self)
