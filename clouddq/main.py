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

"""Data Quality Engine for BigQuery."""
import json
from pathlib import Path
from pprint import pprint
import typing
import logging
import logging.config
import sys
import traceback

import click
import click_logging

from clouddq import lib
from clouddq import utils
from clouddq.bigquery_utils import validate_sql_string
from clouddq.utils import get_template_file


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


APP_NAME = "clouddq"
APP_VERSION = "git rev-parse HEAD"
LOG_LEVEL = logging._nameToLevel["INFO"]


class JsonEncoderStrFallback(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError as exc:
            if "not JSON serializable" in str(exc):
                return str(obj)
            raise


class JsonEncoderDatetime(JsonEncoderStrFallback):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return super().default(obj)


logger = logging.getLogger("clouddq")
logging.basicConfig(
    format="%(json_formatted)s",
    level=LOG_LEVEL,
    handlers=[
        logging.FileHandler(Path('clouddq.log'), 'a'),
        logging.StreamHandler(sys.stderr),
    ],
)
_record_factory_bak = logging.getLogRecordFactory()


def record_factory(*args, **kwargs) -> logging.LogRecord:
    record = _record_factory_bak(*args, **kwargs)
    record.json_formatted = json.dumps(
        {
            "severity": record.levelname,
            "time": record.created,
            "location": "{}:{}:{}".format(
                record.pathname or record.filename,
                record.funcName,
                record.lineno,
            ),
            "exception": record.exc_info,
            "traceback": traceback.format_exception(*record.exc_info)
            if record.exc_info
            else None,
            "message": record.getMessage(),
            "labels": {
                "name": APP_NAME,
                "releaseId": APP_VERSION,
            },
        },
        cls=JsonEncoderDatetime,
    )
    return record


logging.setLogRecordFactory(record_factory)

click_logging_style_kwargs = {
    "debug": dict(fg="cyan", blink=True),
    "info": dict(fg="yellow", blink=True),
    "warn": dict(fg="magenta", blink=True),
    "error": dict(fg="red", blink=True),
    "exception": dict(fg="red", blink=True),
    "critical": dict(fg="red", blink=True),
    }
click_logging_echo_kwargs = {
    "debug": dict(err=True),
    "info": dict(err=True),
    "warn": dict(err=True),
    "error": dict(err=True),
    "exception": dict(err=True),
    "critical": dict(err=True),
    }
click_logging.basic_config(
    logger,
    style_kwargs = click_logging_style_kwargs,
    echo_kwargs = click_logging_echo_kwargs,

)

@click.command()
@click_logging.simple_verbosity_option(logger)
@click.argument("rule_binding_ids")
@click.argument(
    "rule_binding_config_path",
    type=click.Path(exists=True),
)
@click.option(
    "--dbt_profiles_dir",
    help="Path to dbt profiles.yml. "
    "This is a required argument for configuring dbt connection profiles."
    "Defaults to environment variable DBT_PROFILES_DIR if set.",
    type=click.Path(exists=True),
    envvar="DBT_PROFILES_DIR",
    callback=not_null_or_empty,
)
@click.option(
    "--dbt_path",
    help="Path to dbt model directory where a new view will be created "
    "containing the sql execution statement for each rule binding."
    "If not specified, clouddq will created a new directory in "
    "the current working directory for the dbt generated sql files.",
    type=click.Path(exists=True),
    default=None,
)
@click.option(
    "--environment_target",
    help="Execution environment target as defined in dbt profile, "
    "e.g. dev, test, prod.  "
    "Defaults 'dev' or the environment variable ENV if set.",
    envvar="ENV",
    default="dev",
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
    "--debug",
    help="If True, print additional diagnostic information.",
    is_flag=True,
    default=False,
)
@click.option(
    "--print_sql_queries",
    help="If True, print generated SQL queries to stdout.",
    is_flag=True,
    default=False,
)
@click.option(
    "--skip_sql_validation",
    help="If True, skip validation step of generated SQL using BigQuery dry-run.",
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
def main(  # noqa: C901
    rule_binding_ids: str,
    rule_binding_config_path: str,
    dbt_path: str,
    dbt_profiles_dir: str,
    environment_target: str,
    metadata: str,
    dry_run: bool,
    progress_watermark: bool,
    debug: bool = False,
    print_sql_queries: bool = False,
    skip_sql_validation: bool = False,
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
      --dbt_profiles_dir=dbt \\\n
      --metadata='{"test":"test"}' \\\n

    > python bazel-bin/clouddq/clouddq_patched.zip \\\n
      ALL \\\n
      configs/ \\\n
      --metadata='{"test":"test"}' \\\n
      --dbt_profiles_dir=dbt \\\n
      --dbt_path=dbt \\\n
      --environment_target=dev

    """

    if debug:
        logger.setLevel("DEBUG")
    logger.debug("Current working directory: %s", Path().cwd())
    dbt_profiles_dir = Path(dbt_profiles_dir).absolute()
    if not dbt_profiles_dir.joinpath("profiles.yml").is_file():
        raise ValueError(
            f"Cannot find connection `profiles.yml` configurations at "
            f"`dbt_profiles_dir` path: {dbt_profiles_dir}"
        )
    logger.debug("Using 'dbt_profiles_dir': %s", dbt_profiles_dir)
    if not dbt_path:
        dbt_path = Path().cwd().joinpath("dbt").absolute()
        logger.debug(
            "No argument 'dbt_path' provided. Defaulting to use "
            "'dbt' directory in current working directory at: %s",
            dbt_path,
        )
    else:
        dbt_path = Path(dbt_path).absolute()
        if dbt_path.name != "dbt":
            dbt_path = dbt_path / "dbt"
    if not dbt_path.is_dir():
        logger.debug("Creating a new dbt directory at 'dbt_path': %s", dbt_path)
        dbt_path.mkdir(parents=True, exist_ok=True)
    logger.info("Using 'dbt_path': %s", dbt_path)
    dbt_project_path = dbt_path.absolute().joinpath("dbt_project.yml")
    if not dbt_project_path.is_file():
        logger.debug(
            f"Cannot find `dbt_project.yml` configurations in current path: {dbt_project_path}"
            f"Writing templated 'dbt_project.yml' to: {dbt_project_path}"
        )
        dbt_project_path.write_text(get_template_file(Path("dbt", "dbt_project.yml")))
    logger.info("Using 'dbt_project_path': %s", dbt_project_path)
    dbt_main_path = dbt_path / "models" / "data_quality_engine"
    dbt_main_path.mkdir(parents=True, exist_ok=True)
    if not dbt_main_path.joinpath("main.sql").is_file():
        dbt_main_path.joinpath("main.sql").write_text(
            get_template_file(
                Path("dbt").joinpath("models", "data_quality_engine", "main.sql")
            )
        )
    if not dbt_main_path.joinpath("dq_summary.sql").is_file():
        dbt_main_path.joinpath("dq_summary.sql").write_text(
            get_template_file(
                Path("dbt").joinpath("models", "data_quality_engine", "dq_summary.sql")
            )
        )
    dbt_rule_binding_views_path = dbt_path / "models" / "rule_binding_views"
    dbt_rule_binding_views_path.mkdir(parents=True, exist_ok=True)
    logger.info(
                "Writing generated sql to %s/",
                dbt_rule_binding_views_path.absolute(),
            )
    configs_path = Path(rule_binding_config_path)
    logger.debug("Loading rule bindings from: %s", configs_path.absolute())
    metadata = json.loads(metadata)
    dq_summary_table_name = lib.get_bigquery_dq_summary_table_name(
        dbt_profiles_dir, environment_target, dbt_project_path
    )
    logger.info(
        "Writing summary results to GCP table: `%s`. ",
        dq_summary_table_name,
    )
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
        if debug:
            logger.debug(
                "Creating sql string from configs for rule binding: %s",
                rule_binding_id,
            )
            logger.debug("Rule binding config json:")
            pprint(rule_binding_configs)
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
            debug=print_sql_queries,
            progress_watermark=progress_watermark,
        )
        if not skip_sql_validation:
            validate_sql_string(sql_string)
        if debug:
            logger.debug(
                "*** Writing sql to {dbt_rule_binding_views_path.absolute()}/" "%s.sql",
                rule_binding_id,
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
    try:
        logger.info("Running dbt in path: %s", dbt_path)
        utils.run_dbt(
            dbt_path=dbt_path,
            dbt_profile_dir=dbt_profiles_dir,
            configs=configs,
            environment=environment_target,
            debug=debug,
            dry_run=dry_run,
        )
    except Exception as e:
        logger.error("Encountered unexpected error: " + str(e), exc_info=True)


if __name__ == "__main__":
    main()
