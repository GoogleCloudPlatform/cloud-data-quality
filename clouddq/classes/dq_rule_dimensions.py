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

import typing

from clouddq.utils import assert_not_none_or_empty

import clouddq.classes


@dataclass
class DqRuleDimensions:
    """ """

    dimensions: list | None = None    

    @classmethod
    def update_config(
        cls: DqRuleDimensions, config_current: dict, config_new: dict
    ) -> dict:
        return clouddq.classes.update_config_lists(config_current, config_new)
