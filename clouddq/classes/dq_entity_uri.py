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

import logging
import re
import typing

from clouddq.classes.entity_uri_schemes import EntityUriScheme
from clouddq.classes.metadata_registry_defaults import BIGQUERY_URI_FIELDS
from clouddq.classes.metadata_registry_defaults import DATAPLEX_URI_FIELDS
from clouddq.classes.metadata_registry_defaults import SAMPLE_DEFAULT_REGISTRIES_YAML


UNSUPPORTED_URI_CONFIGS = re.compile("[@#?:]")

logger = logging.getLogger(__name__)


@dataclass
class EntityUri:
    """ """

    scheme: EntityUriScheme
    uri_configs_string: str
    default_configs: dict

    @property
    def complete_uri_string(self: EntityUri) -> str:
        return f"{self.scheme.value.lower()}://{self.uri_configs_string}"

    @property
    def configs_dict(self: EntityUri) -> dict:
        entity_uri_list = self.uri_configs_string.split("/")
        all_configs = {}
        if self.default_configs and type(self.default_configs) == dict:
            all_configs.update(self.default_configs)
        uri_dict = dict(zip(entity_uri_list[::2], entity_uri_list[1::2]))
        all_configs.update(uri_dict)
        return all_configs

    def get_configs(self: EntityUri, configs_key: str) -> typing.Any:
        configs_dict = self.configs_dict
        return configs_dict.get(configs_key)

    @classmethod
    def from_uri(
        cls: EntityUri, uri_string: str, default_configs: dict | None = None
    ) -> EntityUri:
        if "://" not in uri_string:
            raise ValueError(
                f"Invalid entity_uri: {uri_string}\n"
                "Example valid URI: "
                "dataplex://projects/project-id/locations/us-central1/"
                "lakes/lake-id/zones/zone-id/entities/entity-id"
            )
        uri_scheme, uri_configs_string = uri_string.split("://")
        scheme = EntityUriScheme.from_scheme(uri_scheme.upper())
        default_scheme_configs = None
        if default_configs:
            default_scheme_configs = default_configs
        entity_uri = EntityUri(
            scheme=scheme,
            uri_configs_string=uri_configs_string,
            default_configs=default_scheme_configs,
        )
        entity_uri.validate()
        return entity_uri

    def to_dict(self: EntityUri) -> dict:
        return {
            "uri": self.complete_uri_string,
            "scheme": self.scheme.value,
            "entity_id": self.get_entity_id(),
            "db_primary_key": self.get_db_primary_key(),
            "configs": self.configs_dict,
        }

    def validate(self: EntityUri) -> None:
        configs = self.configs_dict
        if "*" in self.uri_configs_string:
            raise NotImplementedError(
                f"EntityUri: '{self.complete_uri_string}' "
                f"does not yet support wildcard character: '*'."
            )
        unsupported_configs = UNSUPPORTED_URI_CONFIGS.search(self.uri_configs_string)
        if unsupported_configs:
            raise ValueError(
                f"EntityUri: '{self.complete_uri_string}' "
                f"contains unsupported entity_uri character: '{unsupported_configs.group(0)}'."
            )
        if self.scheme == EntityUriScheme.DATAPLEX:
            self._validate_dataplex_uri_fields(
                config_id=self.complete_uri_string, configs=configs
            )
        elif self.scheme == EntityUriScheme.BIGQUERY:
            self._validate_bigquery_uri_fields(
                config_id=self.complete_uri_string, configs=configs
            )
        else:
            raise NotImplementedError(
                f"EntityUri scheme '{self.scheme}' in entity_uri: "
                f"'{self.complete_uri_string}'"
                " is not yet supported."
            )
        assert "None" not in self.get_db_primary_key()
        assert "None" not in self.get_entity_id()

    def get_entity_id(self: EntityUri) -> str:
        if self.scheme == EntityUriScheme.DATAPLEX:
            return self.get_configs("entities")
        elif self.scheme == EntityUriScheme.BIGQUERY:
            return self.get_db_primary_key()
        else:
            raise NotImplementedError(
                f"EntityUri.get_entity_id() for scheme '{self.scheme}' "
                f"is not yet supported in entity_uri '{self.complete_uri_string}'."
            )

    def get_db_primary_key(self) -> str:
        if self.scheme == EntityUriScheme.DATAPLEX:
            return (
                f"projects/{self.get_configs('projects')}/"
                f"locations/{self.get_configs('locations')}/"
                f"lakes/{self.get_configs('lakes')}/"
                f"zones/{self.get_configs('zones')}/"
                f"entities/{self.get_configs('entities')}"
            )
        elif self.scheme == EntityUriScheme.BIGQUERY:
            return (
                f"projects/{self.get_configs('projects')}/"
                f"datasets/{self.get_configs('datasets')}/"
                f"tables/{self.get_configs('tables')}"
            )
        else:
            raise NotImplementedError(
                f"EntityUri.get_db_primary_key() for scheme '{self.scheme}' "
                f"is not yet supported in entity_uri '{self.complete_uri_string}'."
            )

    def _validate_dataplex_uri_fields(self, config_id: str, configs: dict) -> None:
        expected_fields = DATAPLEX_URI_FIELDS
        for field in expected_fields:
            value = configs.get(field, None)
            if not value:
                raise ValueError(
                    f"Required argument '{field}' not found in  entity_uri: {config_id}.\n"
                    f"Either add '{field}' to the URI or specify default Dataplex configs for "
                    f"projects/locations/lakes/zones as part of a YAML "
                    f"'metadata_default_registries' config, e.g.\n"
                    f"{SAMPLE_DEFAULT_REGISTRIES_YAML}"
                )

    def _validate_bigquery_uri_fields(self, config_id: str, configs: dict) -> None:
        expected_fields = BIGQUERY_URI_FIELDS
        for field in expected_fields:
            value = configs.get(field, None)
            if not value:
                raise ValueError(
                    value,
                    f"Required argument '{field}' not found in entity_uri: {config_id}.\n"
                    f"Either add '{field}' to the URI or specify default Dataplex "
                    f"projects/locations/lakes/zones configs as part of the YAML "
                    f"input config under headings 'metadata_default_registries', e.g.\n"
                    f"{SAMPLE_DEFAULT_REGISTRIES_YAML}",
                )

    def get_table_name(self: EntityUri):
        configs = self.configs_dict
        project_id = configs.get("projects")
        dataset_id = configs.get("datasets")
        table_id = configs.get("tables")
        return f"{project_id}.{dataset_id}.{table_id}"
