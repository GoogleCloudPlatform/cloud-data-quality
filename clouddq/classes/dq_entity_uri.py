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

from clouddq.utils import validate_uri_and_assert


@dataclass
class EntityUri:

    uri: str

    @property
    def scheme(self):
        return self.uri.split("/")[0].replace(":", "")

    @property
    def entity_id(self):
        return self.uri.split("/")[11]

    @property
    def project_id(self):
        return self.uri.split("/")[3]

    @property
    def location(self):
        return self.uri.split("/")[5]

    @property
    def lake(self):
        return self.uri.split("/")[7]

    @property
    def zone(self):
        return self.uri.split("/")[9]

    def get_compound_primary_key(self):
        if self.scheme == "dataplex":
            return f"projects/{self.project_id}/locations/{self.location}/lakes/{self.lake}/zones/{self.zone}/entities/{self.entity_id}"
        else:
            raise NotImplementedError(f"EntityUri.get_compound_primary_key() for scheme {self.scheme} is not yet supported.")

    @property
    def configs(self):
        return {
            "projects": self.project_id,
            "locations": self.location,
            "lakes": self.lake,
            "zones": self.zone,
            "entities": self.entity_id,
        }

    @classmethod
    def from_uri(cls: EntityUri, entity_uri: str) -> EntityUri:

        validate_uri_and_assert(entity_uri=entity_uri)

        return EntityUri(
            uri=entity_uri,
        )

    def to_dict(self: EntityUri) -> dict:
        """
        Serialize Entity URI to dictionary
        Args:
          self: EntityUri:

        Returns:

        """

        output = {
            "uri": self.uri,
            "scheme": self.scheme,
            "entity_id": self.entity_id,
            "compound_primary_key": self.get_compound_primary_key(),
            "configs": self.configs,
        }

        return dict(output)
