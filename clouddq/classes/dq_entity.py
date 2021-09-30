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

import logging

from dataclasses import dataclass

from clouddq.classes.dq_entity_column import DqEntityColumn
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import get_format_string_arguments
from clouddq.utils import get_from_dict_and_assert


ENTITY_CUSTOM_CONFIG_MAPPING = {
    "BIGQUERY": {
        "table_name": "{table_name}",
        "database_name": "{dataset_name}",
        "instance_name": "{project_name}",
    },
}

logger = logging.getLogger(__name__)


def get_custom_entity_configs(
    entity_id: str, configs_map: dict, source_database: str, config_key: str
) -> str:
    entity_configs = ENTITY_CUSTOM_CONFIG_MAPPING.get(source_database)
    if not entity_configs:
        raise NotImplementedError(
            f"Entity Config ID '{entity_id}' has unsupported source_database "
            f"'{source_database}'."
        )
    entity_config_template = entity_configs.get(config_key)
    if not entity_config_template:
        raise NotImplementedError(
            f"Entity Config ID '{entity_id}' with source_database "
            f"'{source_database}' has unsupported config value '{config_key}'."
        )
    entity_config_template_arguments = get_format_string_arguments(
        entity_config_template
    )
    entity_config_arguments = dict()
    for argument in entity_config_template_arguments:
        argument_value = configs_map.get(argument)
        if argument_value:
            entity_config_arguments[argument] = argument_value
    try:
        config_value = entity_config_template.format(**entity_config_arguments)
    except KeyError:
        if config_key in configs_map:
            logger.warning(
                f"Entity Config ID '{entity_id}' with source_database "
                f"'{source_database}' is using deprecated "
                f"config value '{config_key}'.\n"
                f"This will be removed in version 1.0.\n"
                f"Migrate to use the config values "
                f"'{entity_config_template_arguments}' instead."
            )
            config_value = configs_map.get(config_key)
        else:
            raise ValueError(
                f"Entity Config ID '{entity_id}' with source_database "
                f"'{source_database}' has incomplete config values.\n"
                f"Configs required: '{entity_config_template_arguments}'.\n"
                f"Configs supplied: '{configs_map}'."
            )
    return config_value


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
        table_name = get_custom_entity_configs(
            entity_id=entity_id,
            configs_map=kwargs,
            source_database=source_database,
            config_key="table_name",
        )
        database_name = get_custom_entity_configs(
            entity_id=entity_id,
            configs_map=kwargs,
            source_database=source_database,
            config_key="database_name",
        )
        instance_name = get_custom_entity_configs(
            entity_id=entity_id,
            configs_map=kwargs,
            source_database=source_database,
            config_key="instance_name",
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
        # validate environment override
        environment_override = dict()
        for key, value in kwargs.get("environment_override", dict()).items():
            target_env = get_from_dict_and_assert(
                config_id=entity_id,
                kwargs=value,
                key="environment",
                assertion=lambda v: v.lower() == key.lower(),
                error_msg=f"Environment target key {key} must match value.",
            )
            override_configs = value["override"]
            instance_name_override = get_custom_entity_configs(
                entity_id=entity_id,
                configs_map=override_configs,
                source_database=source_database,
                config_key="instance_name",
            )
            database_name_override = get_custom_entity_configs(
                entity_id=entity_id,
                configs_map=override_configs,
                source_database=source_database,
                config_key="database_name",
            )
            try:
                table_name_override = get_custom_entity_configs(
                    entity_id=entity_id,
                    configs_map=override_configs,
                    source_database=source_database,
                    config_key="table_name",
                )
            except ValueError:
                table_name_override = table_name
            environment_override[target_env] = {
                "instance_name": instance_name_override,
                "database_name": database_name_override,
                "table_name": table_name_override,
            }
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
