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
import sqlite3

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError

from clouddq.utils import convert_json_value_to_dict
from clouddq.utils import unnest_object_to_list

import clouddq.classes.dq_entity as dq_entity
import clouddq.classes.dq_entity_uri as dq_entity_uri
import clouddq.classes.dq_row_filter as dq_row_filter
import clouddq.classes.dq_rule as dq_rule
import clouddq.classes.dq_rule_binding as dq_rule_binding
import clouddq.integration.dataplex.clouddq_dataplex as clouddq_dataplex


logger = logging.getLogger(__name__)


@dataclass
class DqConfigsCache:
    _cache_db: Database

    def __init__(self, sqlite3_db_name: str | None = None):
        if sqlite3_db_name:
            cache_db = Database(sqlite3.connect(sqlite3_db_name))
        else:
            cache_db = Database("clouddq_configs.db", recreate=True)
        self._cache_db = cache_db

    def get_table_entity_id(self, entity_id: str) -> dq_entity.DqEntity:
        entity_id = entity_id.upper()
        try:
            logger.debug(f"get_table_entity_id: {entity_id}")
            entity_record = self._cache_db["entities"].get(entity_id)
        except NotFoundError:
            error_message = (
                f"Table Entity_ID: {entity_id} not found in Entities Config Cache."
            )
            raise NotFoundError(error_message)
        convert_json_value_to_dict(entity_record, "environment_override")
        convert_json_value_to_dict(entity_record, "columns")
        entity = dq_entity.DqEntity.from_dict(entity_id, entity_record)
        return entity

    def get_rule_id(self, rule_id: str) -> dq_rule.DqRule:
        rule_id = rule_id.upper()
        try:
            rule_record = self._cache_db["rules"].get(rule_id)
        except NotFoundError:
            error_message = f"Rule_ID: {rule_id} not found in Rules Config Cache."
            logger.error(error_message, exc_info=True)
            raise NotFoundError(error_message)
        convert_json_value_to_dict(rule_record, "params")
        rule = dq_rule.DqRule.from_dict(rule_id, rule_record)
        return rule

    def get_row_filter_id(self, row_filter_id: str) -> dq_row_filter.DqRowFilter:
        row_filter_id = row_filter_id.upper()
        try:
            row_filter_record = self._cache_db["row_filters"].get(row_filter_id)
        except NotFoundError:
            error_message = (
                f"Row Filter ID: {row_filter_id} not found in Filters Config Cache."
            )
            raise NotFoundError(error_message)
        row_filter = dq_row_filter.DqRowFilter.from_dict(
            row_filter_id, row_filter_record
        )
        return row_filter

    def get_rule_binding_id(
        self, rule_binding_id: str
    ) -> dq_rule_binding.DqRuleBinding:
        rule_binding_id = rule_binding_id.upper()
        try:
            rule_binding_record = self._cache_db["rule_bindings"].get(rule_binding_id)
        except NotFoundError:
            error_message = f"Rule_Binding_ID: {rule_binding_id} not found in Rule Bindings Config Cache."
            raise NotFoundError(error_message)
        convert_json_value_to_dict(rule_binding_record, "rule_ids")
        convert_json_value_to_dict(rule_binding_record, "metadata")
        rule_binding = dq_rule_binding.DqRuleBinding.from_dict(
            rule_binding_id, rule_binding_record
        )
        return rule_binding

    def load_all_rule_bindings_collection(self, rule_binding_collection: dict) -> None:
        rule_bindings_rows = unnest_object_to_list(rule_binding_collection)
        for record in rule_bindings_rows:
            if "entity_uri" not in record:
                record.update({"entity_uri": None})
        self._cache_db["rule_bindings"].upsert_all(
            rule_bindings_rows, pk="id"
        ).add_foreign_key("entity_id", "entities", "id").add_foreign_key(
            "row_filter_id", "row_filters", "id"
        )

    def load_all_entities_collection(self, entities_collection: dict) -> None:
        self._cache_db["entities"].upsert_all(
            unnest_object_to_list(entities_collection), pk="id"
        )

    def load_all_row_filters_collection(self, row_filters_collection: dict) -> None:
        self._cache_db["row_filters"].upsert_all(
            unnest_object_to_list(row_filters_collection), pk="id"
        )

    def load_all_rules_collection(self, rules_collection: dict) -> None:
        logger.debug(f"loading rules: {rules_collection}")
        self._cache_db["rules"].upsert_all(
            unnest_object_to_list(rules_collection), pk="id"
        )

    def resolve_dataplex_entity_uris(
        self, client: clouddq_dataplex.CloudDqDataplexClient
    ) -> None:
        for record in self._cache_db.query(
            "select distinct entity_uri, id from rule_bindings where entity_uri is not null"
        ):
            logger.debug(f"Processing record: {record}")
            entity_uri = dq_entity_uri.EntityUri.from_uri(record["entity_uri"])
            dataplex_entity = client.get_dataplex_entity(
                gcp_project_id=entity_uri.project_id,
                location_id=entity_uri.location,
                lake_name=entity_uri.lake,
                zone_id=entity_uri.zone,
                entity_id=entity_uri.entity_id,
            )
            clouddq_entity = dq_entity.DqEntity.from_dataplex_entity(
                dataplex_entity=dataplex_entity
            ).to_dict()
            resolved_entity = unnest_object_to_list(clouddq_entity)
            logger.debug(f"Writing parsed Dataplex Entity to db: {resolved_entity}")
            self._cache_db["entities"].upsert_all(resolved_entity, pk="id", alter=True)
            self._cache_db["rule_bindings"].update(
                record["id"], {"entity_id": resolved_entity[0]["id"]}
            )
