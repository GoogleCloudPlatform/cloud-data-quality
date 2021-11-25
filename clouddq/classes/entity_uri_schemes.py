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
from enum import Enum
from enum import unique


UNSUPPORTED_SCHEMES = ["dataplex:bigquery"]

@unique
class EntityUriScheme(str, Enum):
    """ """
    DATAPLEX = "dataplex"
    BIGQUERY = "bigquery"

    @classmethod
    def from_scheme(cls, scheme: str):

        if scheme in UNSUPPORTED_SCHEMES:
            raise ValueError(f"{scheme} scheme is invalid.")
        else:
            supported_schemes = [supported_scheme.value for supported_scheme in EntityUriScheme]
            if scheme in supported_schemes:
                return cls(scheme)
            else:
                raise NotImplementedError(f"{scheme} scheme is not implemented.")

            # works with python >  3.8

            # if any((uri_scheme := supported_scheme.value) == scheme for supported_scheme in EntityUriScheme):
            #     return cls(uri_scheme)
            # else:
            #     raise NotImplementedError
