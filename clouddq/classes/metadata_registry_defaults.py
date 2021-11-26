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

from clouddq.classes.entity_uri_schemes import EntityUriScheme

DATAPLEX_URI_FIELDS = ["projects", "locations", "lakes", "zones", "entities"]
BIGQUERY_URI_FIELDS = ["projects", "datasets", "tables"]

logger = logging.getLogger(__name__)


@dataclass
class MetadataRegistryDefaults:
    """ """
    default_configs: dict
    @classmethod
    def from_dict(cls: MetadataRegistryDefaults, kwargs: dict) -> MetadataRegistryDefaults:
        default_configs = {}
        logger.debug(f"MetadataRegistryDefaults.from_dict(): {kwargs}")
        for registry_scheme, registry_defaults in kwargs.items():
            scheme = EntityUriScheme.from_scheme(registry_scheme)
            default_configs[scheme.value] = {}
            expected_uri_fields = None
            if scheme == EntityUriScheme.DATAPLEX:
                expected_uri_fields = DATAPLEX_URI_FIELDS
            elif scheme == EntityUriScheme.BIGQUERY:
                expected_uri_fields = BIGQUERY_URI_FIELDS
            for key, value in registry_defaults.items():
                if key not in expected_uri_fields:
                    raise ValueError(
                        f"metadata_registry_default for scheme '{scheme}' "
                        f"contains unexpected field '{key}'."
                        )
                default_configs[scheme][key] = value
        return MetadataRegistryDefaults(default_configs=default_configs)

    def to_dict(self: MetadataRegistryDefaults) -> dict:
        return self.default_configs

    def get_dataplex_registry_defaults(self, key: str | None = None) -> str | None:
        if self.default_configs.get('dataplex', None):
            if key:
                return self.default_configs['dataplex'].get(key, None)
            else:
                return self.default_configs['dataplex']
        else:
            return None
        