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

"""todo: add lib docstring."""
from __future__ import annotations

from pathlib import Path
from pprint import pformat

import itertools
import json
import logging
import typing

from clouddq.classes.dq_config_type import DqConfigType
from clouddq.classes.dq_configs_cache import DqConfigsCache
from clouddq.classes.dq_rule import DqRule
from clouddq.classes.dq_rule_binding import DqRuleBinding
from clouddq.classes.metadata_registry_defaults import MetadataRegistryDefaults
from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import load_jinja_template
from clouddq.utils import load_yaml
from clouddq.utils import sha256_digest


logger = logging.getLogger(__name__)


def load_configs(configs_path: Path, configs_type: DqConfigType) -> dict:

    if configs_path.is_file():
        yaml_files = [configs_path]
    else:
        yaml_files = itertools.chain(
            configs_path.glob("**/*.yaml"), configs_path.glob("**/*.yml")
        )
    all_configs = {}
    for file in yaml_files:
        config = load_yaml(file, configs_type.value)
        if not config:
            continue

        all_configs = DqConfigsCache.update_config(configs_type, all_configs, config)

    if configs_type.is_required():
        assert_not_none_or_empty(
            all_configs,
            f"Failed to load {configs_type.value} from file path: {configs_path}",
        )

    return all_configs


def load_rule_bindings_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.RULE_BINDINGS)


def load_rule_dimensions_config(configs_path: Path) -> list:
    return load_configs(configs_path, DqConfigType.RULE_DIMENSIONS)


def load_entities_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.ENTITIES)


def load_rules_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.RULES)


def load_row_filters_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.ROW_FILTERS)


def load_reference_columns_config(configs_path: Path) -> dict:
    return load_configs(configs_path, DqConfigType.REFERENCE_COLUMNS)


def load_metadata_registry_default_configs(
    configs_path: Path,
) -> MetadataRegistryDefaults:
    configs = load_configs(configs_path, DqConfigType.METADATA_REGISTRY_DEFAULTS)
    try:
        return MetadataRegistryDefaults.from_dict(configs)
    except ValueError as e:
        logger.warning(e)
        return MetadataRegistryDefaults.from_dict({})


def create_rule_binding_view_model(
    rule_binding_id: str,
    rule_binding_configs: dict,
    dq_summary_table_name: str,
    environment: str,
    configs_cache: DqConfigsCache,
    bigquery_client: BigQueryClient,
    dq_summary_table_exists: bool = False,
    metadata: dict | None = None,
    debug: bool = False,
    progress_watermark: bool = True,
    default_configs: dict | None = None,
    high_watermark_filter_exists: bool = False,
) -> dict:
    template = load_jinja_template(
        template_path=Path("dbt", "macros", "create_rule_binding_view.sql")
    )
    failed_records_template = load_jinja_template(
        template_path=Path("dbt", "macros", "failed_records_query.sql")
    )
    configs = prepare_configs_from_rule_binding_id(
        rule_binding_id=rule_binding_id,
        rule_binding_configs=rule_binding_configs,
        dq_summary_table_name=dq_summary_table_name,
        environment=environment,
        configs_cache=configs_cache,
        metadata=metadata,
        progress_watermark=progress_watermark,
        default_configs=default_configs,
        dq_summary_table_exists=dq_summary_table_exists,
        high_watermark_filter_exists=high_watermark_filter_exists,
        bigquery_client=bigquery_client,
    )
    rule_configs_dict = configs.get("configs").get("rule_configs_dict")
    generated_sql_string_dict = dict()
    for rule_id, rule_configs in rule_configs_dict.items():
        configs.update({"rule_type": rule_configs.get("rule_type")})
        configs.update({"rule_id": rule_id})
        failed_records_sql_string = failed_records_template.render(configs)
        existing_configs = configs.get("configs")
        existing_configs[
            f"{rule_binding_id}_{rule_id}_failed_records_sql_string"
        ] = failed_records_sql_string
        configs.update({"configs": existing_configs})
    sql_string = template.render(configs)
    generated_sql_string_dict[f"{rule_binding_id}_generated_sql_string"] = sql_string

    configs.update({"generated_sql_string_dict": generated_sql_string_dict})
    if debug:
        logger.debug(
            f"Prepared json configs for {rule_binding_id}:\n{pformat(configs)}"
        )
    return configs


def create_entity_summary_model(
    entity_table_id: str,
    entity_target_rule_binding_configs: dict,
    gcp_project_id: str,
    gcp_bq_dataset_id: str,
    debug: bool = False,
) -> str:
    if debug:
        logger.info(
            f"Generating Entity-level DQ Summary aggregate for entity "
            f"{entity_table_id} with entity_target_rule_binding_configs:\n"
            f"{pformat(entity_target_rule_binding_configs)}"
        )
    template = load_jinja_template(
        template_path=Path("dbt", "macros", "create_entity_aggregate_dq_summary.sql")
    )
    configs = {
        "entity_target_rule_binding_configs": entity_target_rule_binding_configs,
        "gcp_project_id": gcp_project_id,
        "gcp_bq_dataset_id": gcp_bq_dataset_id,
    }
    sql_string = template.render(configs)
    if debug:
        logger.debug(
            f"Generated sql for entity_table_id: {entity_table_id}:\n{sql_string}"
        )
    return sql_string


def write_sql_string_as_dbt_model(
    model_id: str, sql_string: str, dbt_model_path: Path
) -> None:
    with open(dbt_model_path / f"{model_id}.sql", "w") as f:
        f.write(sql_string.strip())


def prepare_configs_from_rule_binding_id(
    rule_binding_id: str,
    rule_binding_configs: dict,
    dq_summary_table_name: str,
    environment: str | None,
    configs_cache: DqConfigsCache,
    bigquery_client: BigQueryClient,
    dq_summary_table_exists: bool = False,
    metadata: dict | None = None,
    progress_watermark: bool = True,
    default_configs: dict | None = None,
    high_watermark_filter_exists: bool = False,
) -> dict:
    rule_binding = DqRuleBinding.from_dict(
        rule_binding_id, rule_binding_configs, default_configs
    )
    resolved_rule_binding_configs = rule_binding.resolve_all_configs_to_dict(
        configs_cache=configs_cache,
        bigquery_client=bigquery_client,
    )
    configs: dict[typing.Any, typing.Any] = {
        "configs": dict(resolved_rule_binding_configs)
    }
    if environment:
        configs.update({"environment": environment})
    if not metadata:
        metadata = dict()
    if "metadata" in rule_binding_configs:
        metadata.update(rule_binding_configs["metadata"])
    configs.update({"dq_summary_table_name": dq_summary_table_name})
    configs.update({"metadata": metadata})
    configs.update({"dq_summary_table_exists": dq_summary_table_exists})
    configs.update(
        {"configs_hashsum": sha256_digest(json.dumps(resolved_rule_binding_configs))}
    )
    configs.update({"progress_watermark": progress_watermark})
    incremental_time_filter_column = configs["configs"][
        "incremental_time_filter_column"
    ]
    logger.debug(f"Incremental time filter column {incremental_time_filter_column}")
    if incremental_time_filter_column:
        high_watermark_filter_exists = True
        fully_qualified_table_name = (
            f"{configs['configs']['entity_configs']['project_name']}."
            f"{configs['configs']['entity_configs']['dataset_name']}."
            f"{configs['configs']['entity_configs']['table_name']}"
        )
        high_watermark_dict = get_high_watermark_value(
            fully_qualified_table_name=fully_qualified_table_name,
            rule_binding_id=rule_binding_id,
            dq_summary_table_name=dq_summary_table_name,
            bigquery_client=bigquery_client,
            dq_summary_table_exists=dq_summary_table_exists,
        )
        configs.update(high_watermark_dict)
    configs.update({"high_watermark_filter_exists": high_watermark_filter_exists})
    return configs


def prepare_configs_cache(configs_path: Path) -> DqConfigsCache:
    configs_cache = DqConfigsCache()
    entities_collection = load_entities_config(configs_path)
    configs_cache.load_all_entities_collection(entities_collection)
    row_filters_collection = load_row_filters_config(configs_path)
    configs_cache.load_all_row_filters_collection(row_filters_collection)
    reference_columns_collection = load_reference_columns_config(configs_path)
    configs_cache.load_all_reference_columns_collection(reference_columns_collection)
    rule_dimensions_collection = load_rule_dimensions_config(configs_path)
    configs_cache.load_all_rule_dimensions_collection(rule_dimensions_collection)
    rules_collection = load_rules_config(configs_path)

    # validate rules against dimensions
    for rule_id, rule in rules_collection.items():
        DqRule.validate(rule_id, rule, rule_dimensions_collection)

    configs_cache.load_all_rules_collection(rules_collection)
    rule_binding_collection = load_rule_bindings_config(configs_path)
    configs_cache.load_all_rule_bindings_collection(rule_binding_collection)
    return configs_cache


def get_high_watermark_value(
    fully_qualified_table_name: str,
    rule_binding_id: str,
    dq_summary_table_name: str,
    bigquery_client: BigQueryClient,
    dq_summary_table_exists: bool = False,
) -> dict:
    if dq_summary_table_exists:
        query = f"""SELECT
            IFNULL(MAX(execution_ts), TIMESTAMP("1970-01-01 00:00:00")) as high_watermark,
            CURRENT_TIMESTAMP() as current_timestamp_value,
            FROM `{dq_summary_table_name}`
            WHERE table_id = '{fully_qualified_table_name}'
            AND rule_binding_id = '{rule_binding_id}'
            AND progress_watermark IS TRUE ;"""
    else:
        query = f"""SELECT 
            TIMESTAMP("1970-01-01 00:00:00") as high_watermark,
            CURRENT_TIMESTAMP() as current_timestamp_value ;"""
    logger.info(f"High watermark query is \n {query}")
    query_job = bigquery_client.execute_query(query_string=query).result()
    high_watermark_value = ""
    current_timestamp_value = ""
    for row in query_job:
        # Row values can be accessed by field name or index.
        logger.info(f"High watermark value is {row['high_watermark']}")
        high_watermark_value = row["high_watermark"]
        current_timestamp_value = row["current_timestamp_value"]
    out_dict = {
        "high_watermark_value": high_watermark_value,
        "current_timestamp_value": current_timestamp_value,
    }
    return out_dict
