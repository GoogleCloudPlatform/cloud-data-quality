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
from enum import Enum
from enum import unique


@unique
class EntityUriScheme(str, Enum):
    """ """

    DATAPLEX = "DATAPLEX"
    BIGQUERY = "BIGQUERY"

    @classmethod
    def from_scheme(cls, scheme: str):

        try:
            return cls(scheme.upper())
        except ValueError:
            raise NotImplementedError(f"{scheme} scheme is not implemented.")
