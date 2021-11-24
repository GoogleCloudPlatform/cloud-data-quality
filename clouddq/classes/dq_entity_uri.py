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

from clouddq.classes.metadata_registry_defaults import DATAPLEX_URI_FIELDS
from clouddq.classes.metadata_registry_defaults import BIGQUERY_URI_FIELDS
from clouddq.classes.entity_uri_schemes import EntityUriScheme
from clouddq.utils import get_from_dict_and_assert

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
        return f"{self.scheme.value}://{self.uri_configs_string}"
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
    def from_uri(cls: EntityUri, uri_string: str, default_configs: dict | None = None) -> EntityUri:
        uri_scheme, uri_configs_string = uri_string.split("://")
        scheme = EntityUriScheme(uri_scheme)
        default_scheme_configs = None
        if default_configs:
            default_scheme_configs = default_configs
        logger.debug(f"{default_scheme_configs=}")
        entity_uri = EntityUri(scheme=scheme,
            uri_configs_string=uri_configs_string,
            default_configs=default_scheme_configs)
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
                f"EntityUri: '{self.scheme}://{self.uri_configs_string}' "
                f"does not yet support wildcard character: '*'."
                )
        unsupported_configs = UNSUPPORTED_URI_CONFIGS.search(self.uri_configs_string)
        if unsupported_configs:
            raise ValueError(
                f"EntityUri: '{self.scheme}://{self.uri_configs_string}' "
                f"contains unsupported entity_uri character: '{unsupported_configs.group(0)}'."
                )
        if self.scheme == EntityUriScheme.DATAPLEX:
            self._validate_dataplex_uri_fields(
                config_id=self.complete_uri_string, 
                configs=configs)
        elif self.scheme == EntityUriScheme.BIGQUERY:
            self._validate_bigquery_uri_fields(
                config_id=self.complete_uri_string, 
                configs=configs)
        else:
            raise NotImplementedError(
                f"EntityUri scheme '{self.scheme}' in entity_uri: "
                f"'{self.complete_uri_string}'"
                " is not yet supported.")
        assert "None" not in self.get_db_primary_key()
        assert "None" not in self.get_entity_id()
    def get_entity_id(self: EntityUri) -> str:
        if self.scheme == EntityUriScheme.DATAPLEX:
            return self.get_configs('entities')
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
                f"entities/{self.get_configs('entities')}"  # noqa: W503
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
            get_from_dict_and_assert(
                config_id=config_id,
                kwargs=configs,
                key=field,
            )
    def _validate_bigquery_uri_fields(self, config_id: str, configs: dict) -> None:
        expected_fields = BIGQUERY_URI_FIELDS
        for field in expected_fields:
            get_from_dict_and_assert(
                config_id=config_id,
                kwargs=configs,
                key=field,
            )

if __name__ == "__main__":
    dataplex_uri = "dataplex://projects/dataplex-clouddq/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entities/contact_details"
    print(f"{dataplex_uri=}")
    dataplex_entity_uri = EntityUri.from_uri(dataplex_uri)
    print(f"{dataplex_entity_uri.configs_dict=}")
    print(f"{dataplex_entity_uri.get_entity_id()=}")
    print(f"{dataplex_entity_uri.get_db_primary_key()=}")
    print(f"{dataplex_entity_uri.to_dict()=}")
    bigquery_uri = "bigquery://projects/project-id/datasets/dataset-id/tables/table-id"
    print(f"{bigquery_uri=}")
    bigquery_entity_uri = EntityUri.from_uri(bigquery_uri)
    print(f"{bigquery_entity_uri.configs_dict=}")
    print(f"{bigquery_entity_uri.get_entity_id()=}")
    print(f"{bigquery_entity_uri.get_db_primary_key()=}")
    print(f"{bigquery_entity_uri.to_dict()=}")
    try:
        gcs_uri = "gs://bucket-name/tables/table-id"
        print(f"{gcs_uri=}")
        gcs_entity_uri = EntityUri.from_uri(gcs_uri)
        print(f"{gcs_entity_uri=}")
    except NotImplementedError as e:
        print(e)
        pass
    try:
        unsupported_uri = "dataplex://projects/dataplex-clouddq/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entities/contact_details:column"
        print(f"{unsupported_uri=}")
        unsupported_entity_uri = EntityUri.from_uri(unsupported_uri)
        print(f"{unsupported_entity_uri=}")
    except ValueError as e:
        print(e)
        pass
    try:
        unsupported_uri = "dataplex://projects/dataplex-clouddq/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entities/contact_details_*"
        print(f"{unsupported_uri=}")
        unsupported_entity_uri = EntityUri.from_uri(unsupported_uri)
        print(f"{unsupported_entity_uri=}")
    except NotImplementedError as e:
        print(e)
        pass
    try:
        validation_error_uri = "dataplex://projects/dataplex-clouddq/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entitie/contact_details"
        print(f"{validation_error_uri=}")
        validation_error_uri_entity_uri = EntityUri.from_uri(validation_error_uri)
        print(f"{validation_error_uri_entity_uri=}")
    except ValueError as e:
        print(e)
        pass
    try:
        validation_error_uri = "dataplex://dataplex-clouddq/locations/us-central1/lakes/amandeep-dev-lake/zones/raw/entities/contact_details"
        print(f"{validation_error_uri=}")
        validation_error_uri_entity_uri = EntityUri.from_uri(validation_error_uri)
        print(f"{validation_error_uri_entity_uri=}")
    except ValueError as e:
        print(e)
        pass
    try:
        validation_error_uri = "dataplex://projects/dataplex-clouddq/locations//lakes/amandeep-dev-lake/zones/raw/entities/contact_details"
        print(f"{validation_error_uri=}")
        validation_error_uri_entity_uri = EntityUri.from_uri(validation_error_uri)
        print(f"{validation_error_uri_entity_uri=}")
    except ValueError as e:
        print(e)
        pass

# @dataclass
# class EntityUri:

#     uri: str

#     @property
#     def scheme(self):
#         return self.uri.split("/")[0].replace(":", "")

#     @property
#     def entity_id(self):
#         return self.uri.split("/")[11]

#     @property
#     def project_id(self):
#         return self.uri.split("/")[3]

#     @property
#     def location(self):
#         return self.uri.split("/")[5]

#     @property
#     def lake(self):
#         return self.uri.split("/")[7]

#     @property
#     def zone(self):
#         return self.uri.split("/")[9]

#     def get_db_primary_key(self):
#         if self.scheme == "dataplex":
#             return (
#                 f"projects/{self.project_id}/locations/{self.location}/"
#                 + f"lakes/{self.lake}/zones/{self.zone}/entities/{self.entity_id}"  # noqa: W503
#             )
#         else:
#             raise NotImplementedError(
#                 f"EntityUri.get_db_primary_key() for scheme {self.scheme} is not yet supported."
#             )

#     @property
#     def configs(self):
#         return {
#             "projects": self.project_id,
#             "locations": self.location,
#             "lakes": self.lake,
#             "zones": self.zone,
#             "entities": self.entity_id,
#         }

#     @classmethod
#     def from_uri(cls: EntityUri, entity_uri: str) -> EntityUri:

#         cls.validate_uri_and_assert(entity_uri=entity_uri)

#         return EntityUri(
#             uri=entity_uri,
#         )

#     def to_dict(self: EntityUri) -> dict:
#         """
#         Serialize Entity URI to dictionary
#         Args:
#           self: EntityUri:

#         Returns:

#         """

#         output = {
#             "uri": self.uri,
#             "scheme": self.scheme,
#             "entity_id": self.entity_id,
#             "db_primary_key": self.get_db_primary_key(),
#             "configs": self.configs,
#         }

#         return dict(output)

#     @classmethod
#     def convert_entity_uri_to_dict(
#         cls: EntityUri, entity_uri: str, delimiter: str
#     ) -> dict:

#         entity_uri_list = entity_uri.split(delimiter)
#         return dict(zip(entity_uri_list[::2], entity_uri_list[1::2]))

#     @classmethod
#     def validate_uri_and_assert(
#         cls: EntityUri, entity_uri: str
#     ) -> typing.Any:  # noqa: C901

#         entity_uri_dict = cls.convert_entity_uri_to_dict(entity_uri, delimiter="/")

#         scheme = entity_uri.split("//")[0] + "//"
#         entity_uri_without_scheme = entity_uri.split("//")[1]

#         for unsupported_scheme in DataplexUnsupportedSchemes:
#             if scheme == unsupported_scheme.value:
#                 raise NotImplementedError(f"{scheme} scheme is not implemented.")

#         for supported_scheme in DataplexSupportedSchemes:
#             if scheme != supported_scheme.value:
#                 raise ValueError(f"{scheme} scheme is invalid.")

#         else:

#             if "projects" not in entity_uri_dict:
#                 if "locations" not in entity_uri_dict:
#                     if "lakes" not in entity_uri_dict:
#                         if "zones" in entity_uri_dict:
#                             raise NotImplementedError(
#                                 f"{entity_uri} is not implemented."
#                             )

#             if "projects" not in entity_uri_dict:
#                 raise ValueError(f"Invalid Entity URI : {entity_uri}")

#             if "locations" not in entity_uri_dict:
#                 raise ValueError(f"Invalid Entity URI : {entity_uri}")

#             if "lakes" not in entity_uri_dict:
#                 raise ValueError(f"Invalid Entity URI : {entity_uri}")

#             if "zones" not in entity_uri_dict:
#                 raise ValueError(f"Invalid Entity URI : {entity_uri}")

#             if "entities" not in entity_uri_dict:
#                 raise ValueError(f"Invalid Entity URI : {entity_uri}")

#             project_id = entity_uri_dict.get("projects")
#             assert_not_none_or_empty(
#                 value=project_id,
#                 error_msg=f"Cannot find required argument project_id in the URI : {entity_uri}",
#             )

#             location_id = entity_uri_dict.get("locations")
#             assert_not_none_or_empty(
#                 value=location_id,
#                 error_msg=f"Cannot find required argument location_id in the URI : {entity_uri}",
#             )

#             lake_id = entity_uri_dict.get("lakes")
#             assert_not_none_or_empty(
#                 value=lake_id,
#                 error_msg=f"Cannot find required argument lake_id in the URI : {entity_uri}",
#             )

#             zone_id = entity_uri_dict.get("zones")
#             assert_not_none_or_empty(
#                 value=zone_id,
#                 error_msg=f"Cannot find required argument zone_id in the URI : {entity_uri}",
#             )

#             entity_id = entity_uri_dict.get("entities")
#             assert_not_none_or_empty(
#                 value=entity_id,
#                 error_msg=f"Cannot find required argument entity_id in the URI : {entity_uri}",
#             )

#             if entity_id.endswith("*"):
#                 raise NotImplementedError(
#                     f"{entity_id} wildcard filter is not implemented."
#                 )

#             if "@" in entity_uri_without_scheme:
#                 raise ValueError(
#                     f"Invalid Entity URI : {entity_uri}, Special characters [@, #, ?, : ] are not allowed."
#                 )

#             if "#" in entity_uri_without_scheme:
#                 raise ValueError(
#                     f"Invalid Entity URI : {entity_uri}, Special characters [@, #, ?, : ] are not allowed."
#                 )

#             if "?" in entity_uri_without_scheme:
#                 raise ValueError(
#                     f"Invalid Entity URI : {entity_uri}, Special characters [@, #, ?, : ] are not allowed."
#                 )

#             if ":" in entity_uri_without_scheme:
#                 raise ValueError(
#                     f"Invalid Entity URI : {entity_uri}, Special characters [@, #, ?, : ] are not allowed."
#                 )
