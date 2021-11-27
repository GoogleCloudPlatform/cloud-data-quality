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

"""todo: add lib docstring."""
from pathlib import Path
from pprint import pformat

import itertools
import json
import logging
import typing

from clouddq.classes.dq_config_type import DqConfigType
from clouddq.classes.dq_configs_cache import DqConfigsCache
from clouddq.classes.dq_rule_binding import DqRuleBinding
from clouddq.classes.metadata_registry_defaults import MetadataRegistryDefaults
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import load_jinja_template
from clouddq.utils import load_yaml
from clouddq.utils import sha256_digest


logger = logging.getLogger(__name__)


def load_configs(configs_path: Path, configs_type: DqConfigType) -> typing.Dict:
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
        intersection = config.keys() & all_configs.keys()

        # The new config defines keys that we have already loaded
        if intersection:
            # Verify that objects pointed to by duplicate keys are identical
            config_i = {}
            all_configs_i = {}
            for k in intersection:
                config_i[k] = config[k]
                all_configs_i[k] = all_configs[k]

            # == on dicts performs deep compare:
            if not config_i == all_configs_i:
                raise ValueError(
                    f"Detected Duplicated Config ID(s): {intersection} "
                    f"If a config ID is repeated, it must be for an identical "
                    f"configuration."
                )

        all_configs.update(config)

    if configs_type.is_required():
        assert_not_none_or_empty(
            all_configs,
            f"Failed to load {configs_type.value} from file path: {configs_path}",
        )

    return all_configs


def load_rule_bindings_config(configs_path: Path) -> typing.Dict:
    return load_configs(configs_path, DqConfigType.RULE_BINDINGS)


def load_entities_config(configs_path: Path) -> typing.Dict:
    return load_configs(configs_path, DqConfigType.ENTITIES)


def load_rules_config(configs_path: Path) -> typing.Dict:
    return load_configs(configs_path, DqConfigType.RULES)


def load_row_filters_config(configs_path: Path) -> typing.Dict:
    return load_configs(configs_path, DqConfigType.ROW_FILTERS)


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
    rule_binding_configs: typing.Dict,
    dq_summary_table_name: str,
    environment: str,
    configs_cache: DqConfigsCache,
    metadata: typing.Optional[typing.Dict] = None,
    debug: bool = False,
    progress_watermark: bool = True,
    default_configs: typing.Optional[typing.Dict] = None,
) -> str:
    template = load_jinja_template(
        template_path=Path("dbt", "macros", "run_dq_main.sql")
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
    )
    sql_string = template.render(configs)
    if debug:
        configs.update({"generated_sql_string": sql_string})
        logger.debug(pformat(configs))
    return sql_string


def write_sql_string_as_dbt_model(
    rule_binding_id: str, sql_string: str, dbt_rule_binding_views_path: Path
) -> None:
    with open(dbt_rule_binding_views_path / f"{rule_binding_id}.sql", "w") as f:
        f.write(sql_string.strip())


def prepare_configs_from_rule_binding_id(
    rule_binding_id: str,
    rule_binding_configs: typing.Dict,
    dq_summary_table_name: str,
    environment: typing.Optional[str],
    configs_cache: DqConfigsCache,
    metadata: typing.Optional[typing.Dict] = None,
    progress_watermark: bool = True,
    default_configs: typing.Optional[typing.Dict] = None,
) -> typing.Dict:
    rule_binding = DqRuleBinding.from_dict(
        rule_binding_id, rule_binding_configs, default_configs
    )
    resolved_rule_binding_configs = rule_binding.resolve_all_configs_to_dict(
        configs_cache=configs_cache,
    )
    configs: typing.Dict[typing.Any, typing.Any] = {
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
    configs.update({"configs_hashsum": sha256_digest(json.dumps(rule_binding_configs))})
    configs.update({"progress_watermark": progress_watermark})
    logger.debug(f"Prepared json configs for {rule_binding_id}:\n{pformat(configs)}")
    return configs


def prepare_configs_cache(configs_path: Path) -> DqConfigsCache:
    configs_cache = DqConfigsCache()
    entities_collection = load_entities_config(configs_path)
    configs_cache.load_all_entities_collection(entities_collection)
    row_filters_collection = load_row_filters_config(configs_path)
    configs_cache.load_all_row_filters_collection(row_filters_collection)
    rules_collection = load_rules_config(configs_path)
    configs_cache.load_all_rules_collection(rules_collection)
    rule_binding_collection = load_rule_bindings_config(configs_path)
    configs_cache.load_all_rule_bindings_collection(rule_binding_collection)
    return configs_cache
