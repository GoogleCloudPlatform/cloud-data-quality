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

import sqlite3
import typing
from dataclasses import dataclass

from sqlite_utils import Database

from clouddq.classes.dq_entity import DqEntity


@dataclass
class DqConfigsCache:
    _cache_db: Database

    def __init__(self, sqlite3_db_name: str | None):
        if sqlite3_db_name:
            cache_db = Database(sqlite3.connect(sqlite3_db_name))
        else:
            cache_db = Database("clouddq_configs.db", recreate=True)
        self._cache_db = cache_db

    def get_table_entity(self, entity_id: str, default_return_value: typing.Any = None) -> DqEntity:
        try:
            entity_record = self._cache_db['entities'].get(entity_id)
            entity = DqEntity.from_dict(entity_record)
            return entity
        except:
            return default_return_value

    def load_all_rule_bindingd_collection(self, rule_binding_collection: dict) -> None:
        raise NotImplementedError()

    def load_all_entities_collection(self, entities_collection: dict) -> None:
        raise NotImplementedError()

    def load_all_row_filters_collection(self, row_filters_collection: dict) -> None:
        raise NotImplementedError()

    def load_all_rules_collection(self, rules_collection: dict) -> None:
        raise NotImplementedError()

    def resolve_all_remote_configs(self) -> None:
        raise NotImplementedError()