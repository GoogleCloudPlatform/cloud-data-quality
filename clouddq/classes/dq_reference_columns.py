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

from clouddq.utils import assert_not_none_or_empty


@dataclass
class DqReferenceColumns:
    """ """

    reference_columns_id: str
    include_reference_columns: list

    @classmethod
    def from_dict(
        cls: DqReferenceColumns,
        reference_columns_id: str,
        kwargs: dict,
    ) -> DqReferenceColumns:
        """

        Args:
          cls: DqReferenceColumns:
          reference_columns_id: str:
          kwargs: typing.Dict:

        Returns:

        """

        include_reference_columns: list = kwargs.get("include_reference_columns", "")
        assert_not_none_or_empty(
            include_reference_columns,
            f"Reference Columns ID: {reference_columns_id} must define attribute "
            f"'include_reference_columns'.",
        )
        return DqReferenceColumns(
            reference_columns_id=str(reference_columns_id),
            include_reference_columns=include_reference_columns,
        )

    def to_dict(self: DqReferenceColumns) -> dict:
        """

        Args:
          self: DqReferenceColumns:

        Returns:

        """
        return dict(
            {
                f"{self.reference_columns_id}": {
                    "include_reference_columns": self.include_reference_columns
                }
            }
        )

    def dict_values(self: DqReferenceColumns) -> dict:
        """

        Args:
          self: DqReferenceColumns:

        Returns:

        """

        return dict(self.to_dict().get(self.reference_columns_id))
