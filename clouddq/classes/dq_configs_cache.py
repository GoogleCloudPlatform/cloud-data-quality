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
from pprint import pformat

import json
import logging
import re
import sqlite3

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError

from clouddq.classes import dq_reference_columns
from clouddq.classes.metadata_registry_defaults import SAMPLE_DEFAULT_REGISTRIES_YAML
from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.utils import convert_json_value_to_dict
from clouddq.utils import transform_json_str_to_dict
from clouddq.utils import unnest_object_to_list

import clouddq.classes.dq_entity as dq_entity
import clouddq.classes.dq_entity_uri as dq_entity_uri
import clouddq.classes.dq_row_filter as dq_row_filter
import clouddq.classes.dq_rule as dq_rule
import clouddq.classes.dq_rule_binding as dq_rule_binding
import clouddq.classes.dq_rule_dimensions as dq_rule_dimensions
import clouddq.integration.dataplex.clouddq_dataplex as clouddq_dataplex


logger = logging.getLogger(__name__)

RE_NON_ALPHANUMERIC = re.compile(r"[^0-9a-zA-Z_]+")
NUM_RULES_PER_TABLE = 50
GET_ENTITY_SUMMARY_QUERY = """
select
    e.instance_name,
    e.database_name,
    e.table_name,
    rb.column_id,
    group_concat(rb.id, ',') as rule_binding_ids_list,
    group_concat(json_array_length(rb.rule_ids), ',') as rules_per_rule_binding
from
    entities e
inner join
    rule_bindings rb
    on UPPER(e.id) = UPPER(rb.entity_id)
where
    rb.id in ({target_rule_binding_ids_list})
group by
    1,2,3,4
"""
GET_DISTINCT_ENTITY_URIS_SQL = """
select distinct
    entity_uri,
    group_concat(id, ',') as rule_binding_ids_list
from
    rule_bindings
where
    entity_uri is not null
    and id in ({target_rule_binding_ids_list})
group by
    1
"""


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
            logger.debug(
                f"Attempting to get from configs cache table entity_id: {entity_id}"
            )
            entity_record = self._cache_db["entities"].get(entity_id)
        except NotFoundError:
            error_message = (
                f"Table Entity_ID: {entity_id} not found in 'entities' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from entities')))}"
            )
            raise NotFoundError(error_message)
        convert_json_value_to_dict(entity_record, "environment_override")
        convert_json_value_to_dict(entity_record, "columns")
        entity = dq_entity.DqEntity.from_dict(entity_id, entity_record)
        partition_fields = entity.get_partition_fields()
        entity.partition_fields = partition_fields
        return entity

    def get_rule_id(self, rule_id: str) -> dq_rule.DqRule:
        rule_id = rule_id.upper()
        try:
            rule_record = self._cache_db["rules"].get(rule_id)
        except NotFoundError:
            error_message = (
                f"Rule_ID: {rule_id} not found in 'rules' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from rules')))}"
            )
            logger.error(error_message, exc_info=True)
            raise NotFoundError(error_message)
        convert_json_value_to_dict(rule_record, "params")
        rule = dq_rule.DqRule.from_dict(rule_id, rule_record)
        return rule

    def get_rule_dimensions(self) -> dq_rule.DqRuleDimensions:
        try:
            dims = self._cache_db["rule_dimensions"].get("rule_dimension")
        except NotFoundError:
            error_message = (
                f"Rule dimensions not found in config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from rule_dimension')))}"
            )
            logger.error(error_message, exc_info=True)
            raise NotFoundError(error_message)
        return dq_rule_dimensions.DqRuleDimensions(dims)

    def get_row_filter_id(self, row_filter_id: str) -> dq_row_filter.DqRowFilter:
        row_filter_id = row_filter_id.upper()
        try:
            row_filter_record = self._cache_db["row_filters"].get(row_filter_id)
        except NotFoundError:
            error_message = (
                f"Row Filter ID: {row_filter_id} not found in 'row_filters' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from row_filters')))}"
            )
            raise NotFoundError(error_message)
        row_filter = dq_row_filter.DqRowFilter.from_dict(
            row_filter_id, row_filter_record
        )
        return row_filter

    def get_reference_columns_id(
        self, reference_columns_id: str
    ) -> dq_reference_columns.DqReferenceColumns:
        reference_columns_id = reference_columns_id.upper()
        try:
            reference_columns_record = self._cache_db["reference_columns"].get(
                reference_columns_id
            )
            reference_columns_record_obj = transform_json_str_to_dict(
                reference_columns_record
            )
        except NotFoundError:
            error_message = (
                f"Reference Column ID: {reference_columns_id} not found in 'reference_columns' config cache:\n"
                f"List of available Reference Column ID's is \n "
                f"{pformat(list(self._cache_db.query('select id from reference_columns')))}"
            )
            raise NotFoundError(error_message)
        reference_columns = dq_reference_columns.DqReferenceColumns.from_dict(
            reference_columns_id, reference_columns_record_obj
        )
        return reference_columns

    def get_rule_binding_id(
        self, rule_binding_id: str
    ) -> dq_rule_binding.DqRuleBinding:
        rule_binding_id = rule_binding_id.upper()
        try:
            rule_binding_record = self._cache_db["rule_bindings"].get(rule_binding_id)
        except NotFoundError:
            error_message = (
                f"Rule_Binding_ID: {rule_binding_id} not found in 'rule_bindings' config cache:\n"
                f"{pformat(list(self._cache_db.query('select id from rule_bindings')))}"
            )
            raise NotFoundError(error_message)
        convert_json_value_to_dict(rule_binding_record, "rule_ids")
        convert_json_value_to_dict(rule_binding_record, "metadata")
        rule_binding = dq_rule_binding.DqRuleBinding.from_dict(
            rule_binding_id, rule_binding_record
        )
        return rule_binding

    def load_all_rule_bindings_collection(self, rule_binding_collection: dict) -> None:
        logger.debug(
            f"Loading 'rule_bindings' configs into cache:\n{pformat(rule_binding_collection.keys())}"
        )
        rule_bindings_rows = unnest_object_to_list(rule_binding_collection)
        for record in rule_bindings_rows:
            try:
                dq_rule_binding.DqRuleBinding.from_dict(
                    rule_binding_id=record["id"], kwargs=record, validate_uri=False
                )
            except Exception as e:
                raise ValueError(f"Failed to parse Rule Binding with error:\n{e}\n")
            if "entity_uri" not in record:
                record.update({"entity_uri": None})
        self._cache_db["rule_bindings"].upsert_all(
            rule_bindings_rows, pk="id", alter=True
        )

    def load_all_entities_collection(self, entities_collection: dict) -> None:
        logger.debug(
            f"Loading 'entities' configs into cache:\n{pformat(entities_collection.keys())}"
        )
        enriched_entities_configs = {}
        for entity_id, entity_configs in entities_collection.items():
            entity = dq_entity.DqEntity.from_dict(entity_id, entity_configs)
            enriched_entities_configs.update(entity.to_dict())
        logger.debug(
            f"entities_collection:\n{pformat(entities_collection)}\n"
            f"enriched_entities_configs:\n{pformat(enriched_entities_configs)}"
        )
        self._cache_db["entities"].upsert_all(
            unnest_object_to_list(enriched_entities_configs), pk="id", alter=True
        )

    def load_all_row_filters_collection(self, row_filters_collection: dict) -> None:
        logger.debug(
            f"Loading 'row_filters' configs into cache:\n{pformat(row_filters_collection.keys())}"
        )
        for row_filter_id, row_filter_record in row_filters_collection.items():
            try:
                dq_row_filter.DqRowFilter.from_dict(
                    row_filter_id=row_filter_id, kwargs=row_filter_record
                )
            except Exception as e:
                raise ValueError(f"Failed to parse Row Filter with error:\n{e}\n")
        self._cache_db["row_filters"].upsert_all(
            unnest_object_to_list(row_filters_collection), pk="id", alter=True
        )

    def load_all_reference_columns_collection(
        self, reference_columns_collection: dict
    ) -> None:
        logger.debug(
            f"Loading 'reference_columns' configs into cache:\n{pformat(reference_columns_collection.keys())}"
        )
        for (
            reference_columns_id,
            reference_columns_record,
        ) in reference_columns_collection.items():
            try:
                dq_reference_columns.DqReferenceColumns.from_dict(
                    reference_columns_id=reference_columns_id,
                    kwargs=reference_columns_record,
                )
            except Exception as e:
                raise ValueError(
                    f"Failed to parse Reference Columns with error:\n{e}\n"
                )
        self._cache_db["reference_columns"].upsert_all(
            unnest_object_to_list(reference_columns_collection), pk="id", alter=True
        )

    def load_all_rules_collection(self, rules_collection: dict) -> None:
        logger.debug(
            f"Loading 'rules' configs into cache:\n{pformat(rules_collection.keys())}"
        )
        for rules_id, rules_record in rules_collection.items():
            try:
                dq_rule.DqRule.from_dict(rule_id=rules_id, kwargs=rules_record)
            except Exception as e:
                raise ValueError(f"Failed to parse Rule with error:\n{e}\n")
        self._cache_db["rules"].upsert_all(
            unnest_object_to_list(rules_collection), pk="id", alter=True
        )

    def load_all_rule_dimensions_collection(
        self, rule_dimensions_collection: list
    ) -> None:
        logger.debug(
            f"Loading 'rule_dimensions' configs into cache:\n{pformat(rule_dimensions_collection)}"
        )
        self._cache_db["rule_dimensions"].upsert_all(
            [{"rule_dimension": dim.upper()} for dim in rule_dimensions_collection],
            pk="rule_dimension",
            alter=True,
        )

    def resolve_dataplex_entity_uris(  # noqa: C901
        self,
        dataplex_client: clouddq_dataplex.CloudDqDataplexClient,
        bigquery_client: BigQueryClient,
        target_rule_binding_ids: list[str],
        default_configs: dict | None = None,
    ) -> None:
        logger.debug(
            f"Using Dataplex default configs for resolving entity_uris:\n{pformat(default_configs)}"
        )
        target_rule_binding_ids = [id.upper() for id in target_rule_binding_ids]
        query = GET_DISTINCT_ENTITY_URIS_SQL.format(
            target_rule_binding_ids_list=",".join(
                [f"'{id}'" for id in target_rule_binding_ids]
            )
        )
        for record in self._cache_db.query(query):
            logger.info(
                f"Calling Dataplex Metadata API to retrieve schema "
                f"for entity_uri:\n{pformat(record)}"
            )
            try:
                entity_uri = dq_entity_uri.EntityUri.from_uri(
                    uri_string=record["entity_uri"],
                    default_configs=default_configs,
                )
            except Exception as e:
                raise ValueError(f"Failed to parse 'entity_uri' with error:\n{e}")
            logger.debug(f"Parsed entity_uri configs:\n{pformat(entity_uri.to_dict())}")
            clouddq_entity = None
            if entity_uri.scheme == "DATAPLEX":
                clouddq_entity = self._resolve_dataplex_entity_uri(
                    entity_uri=entity_uri,
                    dataplex_client=dataplex_client,
                    bigquery_client=bigquery_client,
                )
            elif entity_uri.scheme == "BIGQUERY":
                clouddq_entity = self._resolve_bigquery_entity_uri(
                    entity_uri=entity_uri,
                    dataplex_client=dataplex_client,
                    bigquery_client=bigquery_client,
                )
            else:
                raise RuntimeError(f"Invalid Entity URI scheme: {entity_uri.scheme}")
            resolved_entity = unnest_object_to_list(clouddq_entity.to_dict())
            logger.debug(f"Writing parsed Dataplex Entity to db: {resolved_entity}")
            self._cache_db["entities"].upsert_all(resolved_entity, pk="id", alter=True)
            entity_configs = {
                "entity_id": entity_uri.get_db_primary_key().upper(),
            }
            rule_binding_ids = record["rule_binding_ids_list"].split(",")
            for rule_binding in rule_binding_ids:
                self._cache_db["rule_bindings"].update(
                    rule_binding, entity_configs, alter=True
                )

    def update_config(
        configs_type: str, config_old: list | dict, config_new: list | dict
    ) -> list | dict:
        if configs_type == "rule_dimensions":
            return DqConfigsCache.update_config_lists(config_old, config_new)
        else:
            return DqConfigsCache.update_config_dicts(config_old, config_new)

    def update_config_dicts(config_old: dict, config_new: dict) -> dict:

        if not config_old and not config_new:
            return {}
        elif not config_old:
            return config_new.copy()
        elif not config_new:
            return config_old.copy()
        else:
            intersection = config_old.keys() & config_new.keys()

            # The new config defines keys that we have already loaded
            if intersection:
                # Verify that objects pointed to by duplicate keys are identical
                config_old_i = {}
                config_new_i = {}
                for k in intersection:
                    config_old_i[k] = config_old[k]
                    config_new_i[k] = config_new[k]

                # == on dicts performs deep compare:
                if not config_old_i == config_new_i:
                    raise ValueError(
                        f"Detected Duplicated Config ID(s): {intersection}. "
                        f"If a config ID is repeated, it must be for an identical "
                        f"configuration.\n"
                        f"Duplicated config contents:\n"
                        f"{pformat(config_old_i)}\n"
                        f"{pformat(config_new_i)}"
                    )

            updated = config_old.copy()
            updated.update(config_new)
            return updated

    def update_config_lists(config_old: list, config_new: list) -> list:

        if not config_old and not config_new:
            return []
        elif not config_old:
            return config_new.copy()
        elif not config_new:
            return config_old.copy()
        else:
            # Both lists contain data. This is only OK if they are identical.
            if not sorted(config_old) == sorted(config_new):
                raise ValueError(
                    f"Detected Duplicated Config: {config_new}."
                    f"If a config is repeated, it must be identical."
                )
            return config_old.copy()

    def get_entities_configs_from_rule_bindings(
        self, target_rule_binding_ids: list[str]
    ) -> dict[str, str]:
        query = GET_ENTITY_SUMMARY_QUERY.format(
            target_rule_binding_ids_list=",".join(
                [f"'{id.upper()}'" for id in target_rule_binding_ids]
            )
        )
        target_entity_summary_views_configs = {}
        entity_summary = self._cache_db.query(query)
        if entity_summary:
            for record in entity_summary:
                num_rules_per_table_count = 0
                entity_table_id = "__".join(
                    [
                        record["instance_name"],
                        record["database_name"],
                        record["table_name"],
                        record["column_id"],
                    ]
                )
                entity_table_id = re.sub(RE_NON_ALPHANUMERIC, "_", entity_table_id)
                if len(entity_table_id) > 1023:
                    entity_table_id = entity_table_id[-1022:]
                rule_binding_ids = record["rule_binding_ids_list"].split(",")
                rules_per_rule_binding = record["rules_per_rule_binding"].split(",")
                in_scope_rule_bindings = []
                table_increment = 0
                for index in range(0, len(rules_per_rule_binding)):
                    in_scope_rule_bindings.append(rule_binding_ids[index])
                    num_rules_per_table_count += int(rules_per_rule_binding[index])
                    if num_rules_per_table_count > NUM_RULES_PER_TABLE:
                        table_increment += 1
                        target_entity_summary_views_configs[
                            f"{entity_table_id}_{table_increment}"
                        ] = {
                            "rule_binding_ids_list": in_scope_rule_bindings.copy(),
                        }
                        in_scope_rule_bindings = []
                        num_rules_per_table_count = 0
                else:
                    if in_scope_rule_bindings:
                        table_increment += 1
                        target_entity_summary_views_configs[
                            f"{entity_table_id}_{table_increment}"
                        ] = {
                            "rule_binding_ids_list": in_scope_rule_bindings.copy(),
                        }
            logger.debug(
                f"target_entity_summary_views_configs:\n"
                f"{pformat(target_entity_summary_views_configs)}"
            )
            return target_entity_summary_views_configs

        else:
            raise ValueError(
                f"""Failed to retrieve entity and target rule binding id's \n
                    {target_rule_binding_ids} associated with it"""
            )

    def _resolve_dataplex_entity_uri(
        self,
        entity_uri: dq_entity_uri.EntityUri,
        dataplex_client: clouddq_dataplex.CloudDqDataplexClient,
        bigquery_client: BigQueryClient,
    ) -> dq_entity.DqEntity:
        dataplex_entity = dataplex_client.get_dataplex_entity(
            gcp_project_id=entity_uri.get_configs("projects"),
            location_id=entity_uri.get_configs("locations"),
            lake_name=entity_uri.get_configs("lakes"),
            zone_id=entity_uri.get_configs("zones"),
            entity_id=entity_uri.get_entity_id(),
        )
        clouddq_entity = dq_entity.DqEntity.from_dataplex_entity(
            entity_id=entity_uri.get_db_primary_key(),
            dataplex_entity=dataplex_entity,
        )
        entity_uri_primary_key = entity_uri.get_db_primary_key().upper()
        gcs_entity_external_table_name = clouddq_entity.get_table_name()
        logger.debug(
            f"GCS Entity External Table Name is {gcs_entity_external_table_name}"
        )
        bq_table_exists = bigquery_client.is_table_exists(
            table=gcs_entity_external_table_name,
            project_id=clouddq_entity.instance_name,
        )
        if bq_table_exists:
            logger.debug(
                f"The External Table {gcs_entity_external_table_name} for Entity URI "
                f"{entity_uri_primary_key} exists in Bigquery."
            )
        else:
            raise RuntimeError(
                f"Unable to find Bigquery External Table  {gcs_entity_external_table_name} "
                f"for Entity URI {entity_uri_primary_key}"
            )
        return clouddq_entity

    def is_dataplex_entity(
        self,
        entity_uri: dq_entity_uri.EntityUri,
        dataplex_client: clouddq_dataplex.CloudDqDataplexClient,
    ):
        required_arguments = ["projects", "lakes", "locations", "zones"]
        for argument in required_arguments:
            uri_argument = entity_uri.get_configs(argument)
            if not uri_argument:
                logger.info(
                    f"Failed to retrieve default Dataplex '{argument}' for "
                    f"entity_uri: {entity_uri.complete_uri_string}. \n"
                    f"'{argument}' is a required argument to look-up metadata for the entity_uri "
                    "using Dataplex Metadata API.\n"
                    "Ensure the BigQuery dataset containing this table "
                    "is attached as an asset in Dataplex.\n"
                    "You can then specify the corresponding Dataplex "
                    "projects/locations/lakes/zones as part of the "
                    "metadata_default_registries YAML configs, e.g.\n"
                    f"{SAMPLE_DEFAULT_REGISTRIES_YAML}"
                )
                return False
        dataplex_entities_match = dataplex_client.list_dataplex_entities(
            gcp_project_id=entity_uri.get_configs("projects"),
            location_id=entity_uri.get_configs("locations"),
            lake_name=entity_uri.get_configs("lakes"),
            zone_id=entity_uri.get_configs("zones"),
            data_path=entity_uri.get_entity_id(),
        )
        logger.info(f"Retrieved Dataplex Entities:\n{pformat(dataplex_entities_match)}")
        if len(dataplex_entities_match) != 1:
            logger.info(
                "Failed to retrieve Dataplex Metadata entry for "
                f"entity_uri '{entity_uri.complete_uri_string}' "
                f"with error:\n"
                f"{pformat(json.dumps(dataplex_entities_match))}\n\n"
                f"Parsed entity_uri configs:\n"
                f"{pformat(entity_uri.to_dict())}\n\n"
            )
            return False
        else:
            dataplex_entity = dataplex_entities_match[0]
            clouddq_entity = dq_entity.DqEntity.from_dataplex_entity(
                entity_id=entity_uri.get_db_primary_key(),
                dataplex_entity=dataplex_entity,
            )
        return clouddq_entity

    def _resolve_bigquery_entity_uri(
        self,
        entity_uri: dq_entity_uri.EntityUri,
        dataplex_client: clouddq_dataplex.CloudDqDataplexClient,
        bigquery_client: BigQueryClient,
    ) -> dq_entity.DqEntity:
        required_arguments = ["projects", "datasets", "tables"]
        for argument in required_arguments:
            uri_argument = entity_uri.get_configs(argument)
            if not uri_argument:
                raise RuntimeError(
                    f"Failed to retrieve default Bigquery '{argument}' for "
                    f"entity_uri: {entity_uri.complete_uri_string}. \n"
                    f"'{argument}' is a required argument to look-up metadata for the entity_uri "
                    "using Bigquery API.\n"
                )

        project_id = entity_uri.get_configs("projects")
        table_name = entity_uri.get_table_name()
        bq_table_exists = bigquery_client.is_table_exists(
            table=table_name, project_id=project_id
        )
        if bq_table_exists:
            logger.debug(
                f"The Table '{table_name}' in the "
                f"specified entity_uri '{entity_uri}' "
                f"exists in Bigquery."
            )
            dataplex_entity = self.is_dataplex_entity(
                entity_uri=entity_uri, dataplex_client=dataplex_client
            )
            if dataplex_entity:
                clouddq_entity = dataplex_entity
            else:
                clouddq_entity = dq_entity.DqEntity.from_bq_entity_uri(
                    entity_uri=entity_uri,
                    bigquery_client=bigquery_client,
                )
            return clouddq_entity
        else:
            raise RuntimeError(
                f"Bigquery Table '{table_name}' specified in the "
                f"entity uri '{entity_uri}' does not exist"
            )
