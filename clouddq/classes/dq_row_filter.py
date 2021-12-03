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

import clouddq.classes


@dataclass
class DqRowFilter:
    """ """

    row_filter_id: str
    filter_sql_expr: str

    @classmethod
    def from_dict(
        cls: DqRowFilter,
        row_filter_id: str,
        kwargs: dict,
    ) -> DqRowFilter:
        """

        Args:
          cls: DqRowFilter:
          row_filter_id: str:
          kwargs: typing.Dict:

        Returns:

        """

        filter_sql_expr: str = kwargs.get("filter_sql_expr", "")
        assert_not_none_or_empty(
            filter_sql_expr,
            f"Row Filter ID: {row_filter_id} must define attribute "
            f"'filter_sql_expr'.",
        )
        return DqRowFilter(
            row_filter_id=str(row_filter_id),
            filter_sql_expr=filter_sql_expr,
        )

    def to_dict(self: DqRowFilter) -> dict:
        """

        Args:
          self: DqRowFilter:

        Returns:

        """

        return dict(
            {
                f"{self.row_filter_id}": {
                    "filter_sql_expr": self.filter_sql_expr,
                }
            }
        )

    def dict_values(self: DqRowFilter) -> dict:
        """

        Args:
          self: DqRowFilter:

        Returns:

        """

        return dict(self.to_dict().get(self.row_filter_id))
