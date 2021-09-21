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
import itertools
import json
from pathlib import Path
from pprint import pprint
import typing

import yaml

from clouddq.classes.dq_config_type import DqConfigType
from clouddq.classes.dq_rule_binding import DqRuleBinding
from clouddq.runners.dbt.dbt_utils import extract_dbt_env_var
from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import load_jinja_template
from clouddq.utils import sha256_digest


def load_yaml(file_path: Path, key: str = None) -> typing.Dict:
    with file_path.open() as f:
        yaml_configs = yaml.safe_load(f)
    if not yaml_configs:
        return dict()
    return yaml_configs.get(key, dict())


def get_bigquery_dq_summary_table_name(
    dbt_profile_dir: Path, environment_target: str, dbt_path: Path
) -> str:
    # Get bigquery project and dataset for dq_summary table names
    dbt_project_path = dbt_path.joinpath("dbt_project.yml")
    if not dbt_project_path.is_file():
        raise ValueError(
            "Not able to find 'dbt_project.yml' config file at "
            "input path {dbt_project_path}."
        )
    dbt_profiles_key = load_yaml(dbt_project_path, "profile")
    dbt_profiles_config = load_yaml(dbt_profile_dir / "profiles.yml", dbt_profiles_key)
    dbt_profile = dbt_profiles_config["outputs"][environment_target]
    dbt_project = dbt_profile["project"]
    if "{{" in dbt_project:
        dbt_project = extract_dbt_env_var(dbt_project)
    dbt_dataset = dbt_profile["dataset"]
    if "{{" in dbt_dataset:
        dbt_dataset = extract_dbt_env_var(dbt_dataset)
    dq_summary_table_name = f"{dbt_project}.{dbt_dataset}.dq_summary"
    return dq_summary_table_name


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
        if intersection:
            raise ValueError(
                f"Detected Duplicated Config ID(s): {intersection} "
                f"Config IDs must be unique."
            )
        all_configs.update(config)
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


def create_rule_binding_view_model(
    rule_binding_id: str,
    rule_binding_configs: typing.Dict,
    dq_summary_table_name: str,
    environment: str,
    configs_path: Path,
    entities_collection: typing.Optional[typing.Dict] = None,
    row_filters_collection: typing.Optional[typing.Dict] = None,
    rules_collection: typing.Optional[typing.Dict] = None,
    metadata: typing.Optional[typing.Dict] = None,
    debug: bool = False,
    progress_watermark: bool = True,
) -> str:
    template = load_jinja_template(
        template_path=Path("dbt", "macros", "run_dq_main.sql")
    )
    configs = prepare_configs_from_rule_binding_id(
        rule_binding_id=rule_binding_id,
        rule_binding_configs=rule_binding_configs,
        dq_summary_table_name=dq_summary_table_name,
        environment=environment,
        configs_path=configs_path,
        entities_collection=entities_collection,
        row_filters_collection=row_filters_collection,
        rules_collection=rules_collection,
        metadata=metadata,
        progress_watermark=progress_watermark,
    )
    sql_string = template.render(configs)
    if debug:
        configs.update({"generated_sql_string": sql_string})
        pprint(configs)
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
    configs_path: typing.Optional[Path],
    entities_collection: typing.Optional[typing.Dict] = None,
    row_filters_collection: typing.Optional[typing.Dict] = None,
    rules_collection: typing.Optional[typing.Dict] = None,
    metadata: typing.Optional[typing.Dict] = None,
    progress_watermark: bool = True,
) -> typing.Dict:
    (
        entities_collection,
        row_filters_collection,
        rules_collection,
    ) = load_configs_if_not_defined(
        configs_path=configs_path,
        entities_collection=entities_collection,
        row_filters_collection=row_filters_collection,
        rules_collection=rules_collection,
    )
    rule_binding = DqRuleBinding.from_dict(rule_binding_id, rule_binding_configs)
    resolved_rule_binding_configs = rule_binding.resolve_all_configs_to_dict(
        entities_collection=entities_collection,
        row_filters_collection=row_filters_collection,
        rules_collection=rules_collection,
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
    configs.update({"configs_hashsum": sha256_digest(json.dumps(configs))})
    configs.update({"progress_watermark": progress_watermark})
    return configs


def load_configs_if_not_defined(
    configs_path: Path,
    entities_collection: typing.Dict = None,
    row_filters_collection: typing.Dict = None,
    rules_collection: typing.Dict = None,
) -> typing.Tuple[typing.Dict, typing.Dict, typing.Dict]:
    if not entities_collection:
        entities_collection = load_entities_config(configs_path)
    if not row_filters_collection:
        row_filters_collection = load_row_filters_config(configs_path)
    if not rules_collection:
        rules_collection = load_rules_config(configs_path)
    return entities_collection, row_filters_collection, rules_collection
