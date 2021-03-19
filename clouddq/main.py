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

"""todo: add main docstring."""
import json
import logging
from pathlib import Path
import typing

import click

from clouddq import lib
from clouddq import utils
from clouddq.bigquery_utils import validate_sql_string


def not_null_or_empty(
    ctx: click.Context,
    param: typing.Union[click.core.Option, click.core.Parameter],
    value: typing.Any,
) -> typing.Any:
    if value:
        return value
    else:
        raise click.BadParameter(
            f"Variable {param} must not be empty or none. Input value: {value}"
        )


@click.command()
@click.argument("rule_binding_ids")
@click.argument(
    "rule_binding_config_path",
    type=click.Path(exists=True),
)
@click.option(
    "--dbt_profiles_dir",
    help="Path to dbt profiles.yml. "
    "Defaults to environment variable DBT_PROFILES_DIR if set.",
    type=click.Path(exists=True),
    envvar="DBT_PROFILES_DIR",
)
@click.option(
    "--dbt_path",
    help="Path to dbt model directory where a new view will be created "
    "containing the sql execution statement for each rule binding.",
    type=click.Path(exists=True),
)
@click.option(
    "--environment_target",
    help="Execution environment target as defined in dbt profile, "
    "e.g. dev, test, prod.  "
    "Defaults to environment variable ENV if set.",
    envvar="ENV",
    callback=not_null_or_empty,
)
@click.option(
    "--metadata",
    help="JSON String containing run metadata for each DQ Outputs row. "
    "Useful for allowing custom aggregations over DQ Outputs table "
    "over the metadata attributes.",
    default="{}",
)
@click.option(
    "--dry_run",
    help="If True, do everything except run dbt itself.",
    is_flag=True,
    default=False,
)
@click.option(
    "--progress_watermark",
    help="Whether to set 'progress_watermark' column value "
    "to True/False in dq_summary. Defaults to True.",
    type=bool,
    default=True,
)
def main(
    rule_binding_ids: str,
    rule_binding_config_path: str,
    dbt_path: str,
    dbt_profiles_dir: str,
    environment_target: str,
    metadata: str,
    dry_run: bool,
    progress_watermark: bool,
    debug: bool = False,
) -> None:
    """Run RULE_BINDING_IDS from a RULE_BINDING_CONFIG_PATH.

    RULE_BINDING_IDS:
    comma-separated Rule Binding ID(s) containing the
    configurations for the run.

    Set RULE_BINDING_IDS to 'ALL' to run all rule_bindings
    in RULE_BINDING_CONFIG_PATH.

    RULE_BINDING_CONFIG_PATH:
    Path to YAML configs directory containing `rule_bindings`,
    `entities`, `rules`, and `row_filters` YAML config files.

    Usage examples:

    > python clouddq \\\n
      T2_DQ_1_EMAIL \\\n
      configs/rule_bindings/team-2-rule-bindings.yml \\\n
      --metadata='{"test":"test"}' \\\n
      --dbt_profiles_dir=. \\\n
      --dbt_path=dbt \\\n
      --environment_target=dev

    > python bazel-bin/clouddq/clouddq_patched.zip \\\n
      ALL \\\n
      configs/ \\\n
      --metadata='{"test":"test"}' \\\n
      --dbt_profiles_dir=. \\\n
      --dbt_path=dbt \\\n
      --environment_target=dev

    """
    dbt_profiles_dir = Path(dbt_profiles_dir)
    configs_path = Path(rule_binding_config_path)
    dbt_path = Path(dbt_path)
    metadata = json.loads(metadata)
    dq_summary_table_name = lib.get_dq_summary_table_name(dbt_profiles_dir)
    dbt_rule_binding_views_path = dbt_path / "models" / "rule_binding_views"
    logging.info(
        f"Using BigQuery table for summary results: `{dq_summary_table_name}`. "
    )
    logging.info(f"Loading rule bindings from: {configs_path.absolute()} ")
    all_rule_bindings = lib.load_rule_bindings_config(Path(configs_path))
    target_rule_binding_ids = [r.strip() for r in rule_binding_ids.split(",")]
    if len(target_rule_binding_ids) == 1 and target_rule_binding_ids[0] == "ALL":
        target_rule_binding_ids = list(all_rule_bindings.keys())
    (
        entities_collection,
        row_filters_collection,
        rules_collection,
    ) = lib.load_configs_if_not_defined(
        configs_path=configs_path,
    )
    for rule_binding_id in target_rule_binding_ids:
        rule_binding_configs = all_rule_bindings.get(rule_binding_id, None)
        utils.assert_not_none_or_empty(
            rule_binding_configs,
            f"Target Rule Binding Id: {rule_binding_id} not found "
            f"in config path {configs_path.absolute()}.",
        )
        logging.info(
            f"*** Creating sql string from configs for rule binding: {rule_binding_id}"
        )
        sql_string = lib.create_rule_binding_view_model(
            rule_binding_id=rule_binding_id,
            rule_binding_configs=rule_binding_configs,
            dq_summary_table_name=dq_summary_table_name,
            entities_collection=entities_collection,
            rules_collection=rules_collection,
            row_filters_collection=row_filters_collection,
            configs_path=configs_path,
            environment=environment_target,
            metadata=metadata,
            debug=debug,
            progress_watermark=progress_watermark,
        )
        validate_sql_string(sql_string)
        logging.info(
            f"*** Writing sql to {dbt_rule_binding_views_path.absolute()}/"
            f"{rule_binding_id}.sql"
        )
        lib.write_sql_string_as_dbt_model(
            rule_binding_id=rule_binding_id,
            sql_string=sql_string,
            dbt_rule_binding_views_path=dbt_rule_binding_views_path,
        )
    # clean up old rule_bindings
    for view in dbt_rule_binding_views_path.glob("*.sql"):
        if view.stem not in target_rule_binding_ids:
            view.unlink()
    configs = {"target_rule_binding_ids": target_rule_binding_ids}
    utils.run_dbt(
        dbt_profile_dir=dbt_profiles_dir,
        configs=configs,
        environment=environment_target,
        dry_run=dry_run,
    )


if __name__ == "__main__":
    main()
