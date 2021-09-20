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
from pathlib import Path
from pprint import pprint
import typing
import logging
import logging.config

import click
import click_logging

from clouddq import lib
from clouddq.bigquery_utils import validate_sql_string
from clouddq.utils import assert_not_none_or_empty
from clouddq.runners.dbt import DbtRunner

import json
import logging
import sys
import traceback

from datetime import datetime

APP_NAME = 'clouddq'
APP_VERSION = 'git rev-parse HEAD'
LOG_LEVEL = logging._nameToLevel['INFO']

class JsonEncoderStrFallback(json.JSONEncoder):
  def default(self, obj):
    try:
      return super().default(obj)
    except TypeError as exc:
      if 'not JSON serializable' in str(exc):
        return str(obj)
      raise


class JsonEncoderDatetime(JsonEncoderStrFallback):
  def default(self, obj):
    if isinstance(obj, datetime):
      return obj.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    else:
      return super().default(obj)

logger = logging.getLogger('clouddq')
logging.basicConfig(
  format='%(json_formatted)s',
  level=LOG_LEVEL,
  handlers=[
    #logging.FileHandler('/var/log/clouddq.log', 'a'),
    logging.StreamHandler(sys.stderr),
  ],
)

_record_factory_bak = logging.getLogRecordFactory()
def record_factory(*args, **kwargs) -> logging.LogRecord:
  record = _record_factory_bak(*args, **kwargs)

  record.json_formatted = json.dumps(
    {
      'severity': record.levelname,
      'time': record.created,
      'location': '{}:{}:{}'.format(
        record.pathname or record.filename,
        record.funcName,
        record.lineno,
      ),
      'exception': record.exc_info,
      'traceback': traceback.format_exception(*record.exc_info) if record.exc_info else None,
      'message': record.getMessage(),
      'labels': {
        'name': APP_NAME,
        'releaseId': APP_VERSION,
      },
    },
    cls=JsonEncoderDatetime,
  )
  return record
logger.setLogRecordFactory(record_factory)

click_logging_style_kwargs = {
    'debug': dict(fg='cyan', blink=True),
    'info': dict(fg='yellow', blink=True),
    'warn': dict(fg='magenta', blink=True),
    'error': dict(fg='red', blink=True),
    'exception': dict(fg='red', blink=True),
    'critical': dict(fg='red', blink=True)
}
click_logging_echo_kwargs = {
    'debug': dict(err=True),
    'info': dict(err=True),
    'warn': dict(err=True),
    'error': dict(err=True),
    'exception': dict(err=True),
    'critical': dict(err=True),
}
click_logging.basic_config(
    logger,
    style_kwargs=click_logging_style_kwargs,
    echo_kwargs=click_logging_echo_kwargs
)
@click.command()
@click_logging.simple_verbosity_option(logger)
@click.argument("rule_binding_ids")
@click.argument(
    "rule_binding_config_path",
    type=click.Path(exists=True),
)
# @click.option(
#     "--target_bigquery_summary_table",
#     help="",
# )
@click.option(
    "--gcp_oauth_connection_configs",
    help="Connection profile for authenticating to GCP using credentials "
    "automatically discovered via Application Default Credentials (ADC). "
    "More on how ADC discovers credentials: https://google.aip.dev/auth/4110. "
    "Takes as input a 3-value tuple of: "
    " - GCP Project ID: used for executing GCP Jobs "
    " - GCP Region: GCP region used for running GCP Jobs and for storing "
    " any intemediate DQ summary results "
    " - GCP Dataset ID: used for storing rule_binding views and intermediate DQ summary results "
    "Example usage: "
    "`--gcp_oauth_connection_configs $PROJECT_ID $BIGQUERY_DATASET $BIGQUERY_REGION`"
    "Only one CLI argument for GCP connection config with format "
    "--gcp_{*}_connection_configs can be configured per run. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    type=(str, str, str)
)
@click.option(
    "--gcp_sa_keys_connection_configs",
    help="Connection profile for authenticating to GCP using "
    "a local service account JSON key. "
    "Takes as input a 4-value tuple of: "
    " - GCP Project ID: used for executing GCP Jobs "
    " - GCP Region: GCP region used for running GCP Jobs and for storing "
    " any intemediate DQ summary results "
    " - GCP Dataset ID: used for storing rule_binding views and intermediate DQ summary results "
    " - Service Account Key Path: file system path to the GCP service account JSON key"
    "Example usage: "
    "`--gcp_sa_keys_connection_configs $PROJECT_ID $BIGQUERY_DATASET $BIGQUERY_REGION /path/to/gcp/keyfile.json`"
    "Only one CLI argument for GCP connection config with format "
    "--gcp_{*}_connection_configs can be configured per run. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    type=(str, str, str, str)
)
@click.option(
    "--gcp_sa_impersonation_connection_configs",
    help="Connection profile for authenticating to GCP using "
    "service account impersonation via a local ADC credentials. "
    "Ensure the local ADC credentials has permission to impersonate "
    "the service account such as `roles/iam.serviceAccountTokenCreator`."
    "Takes as input a 4-value tuple of: "
    " - GCP Project ID: used for executing GCP Jobs "
    " - GCP Region: GCP region used for running GCP Jobs and for storing "
    " any intemediate DQ summary results "
    " - GCP Dataset ID: used for storing rule_binding views and intermediate DQ summary results "
    " - Service Account Name: name of the service account you want to impersonate"
    "Example usage: "
    "`--gcp_sa_keys_connection_configs $PROJECT_ID $BIGQUERY_DATASET $BIGQUERY_REGION /path/to/gcp/keyfile.json`"
    "Only one CLI argument for GCP connection config with format "
    "--gcp_{*}_connection_configs can be configured per run. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    type=(str, str, str, str)
)
@click.option(
    "--dbt_profiles_dir",
    help="Path to dbt profiles.yml. "
    "This is a required argument for configuring dbt connection profiles "
    "if another argument --{*}_connection_configs was not provided."
    "Defaults to environment variable DBT_PROFILES_DIR if set. "
    "If set, you can also specify the profile target with --environment_target.",
    type=click.Path(exists=True),
    envvar="DBT_PROFILES_DIR",
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
    "Defaults 'dev' or the environment variable ENV if set. "
    "Only relevant if --dbt_profiles_dir is set. ",
    envvar="ENV",
    default="dev",
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
    help="If True, skip validation step of generated SQL using GCP dry-run.",
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
    gcp_oauth_connection_configs: typing.Tuple[str, str, str],
    gcp_sa_keys_connection_configs: typing.Tuple[str, str, str, str],
    gcp_sa_impersonation_connection_configs: typing.Tuple[str, str, str, str],
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
    dbt_runner = DbtRunner(
        dbt_path=dbt_path,
        dbt_profiles_dir=dbt_profiles_dir,
        environment_target=environment_target,
        gcp_oauth_connection_configs=gcp_oauth_connection_configs,
        gcp_sa_keys_connection_configs=gcp_sa_keys_connection_configs,
        gcp_sa_impersonation_connection_configs=gcp_sa_impersonation_connection_configs,
        debug=debug)
    # Load metadata
    metadata = json.loads(metadata)
    # Prepare DQ Summary Table
    dq_summary_table_name = lib.get_bigquery_dq_summary_table_name(
        dbt_profiles_dir, environment_target, dbt_path
    )
    logger.info(
        "Writing summary results to GCP table: `%s`. ",
        dq_summary_table_name,
    )
    # Load Rule Bindings
    configs_path = Path(rule_binding_config_path)
    if debug:
        logger.debug(
            "Loading rule bindings from: %s", configs_path.absolute()
        )
    all_rule_bindings = lib.load_rule_bindings_config(Path(configs_path))
    # Prepare list of Rule Bindings in-scope for run
    target_rule_binding_ids = [r.strip() for r in rule_binding_ids.split(",")]
    if len(target_rule_binding_ids) == 1 and target_rule_binding_ids[0] == "ALL":
        target_rule_binding_ids = list(all_rule_bindings.keys())
    # Load all other configs
    (
        entities_collection,
        row_filters_collection,
        rules_collection,
    ) = lib.load_configs_if_not_defined(
        configs_path=configs_path,
    )
    for rule_binding_id in target_rule_binding_ids:
        rule_binding_configs = all_rule_bindings.get(rule_binding_id, None)
        assert_not_none_or_empty(
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
                "*** Writing sql to {dbt_rule_binding_views_path.absolute()}/"
                "%s.sql",
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
        dbt_runner.run(
            configs=configs,
            debug=debug,
            dry_run=dry_run
        )
    except Exception as e:
        logger.error("Encountered unexpected error: " + str(e), exc_info=True)


if __name__ == "__main__":
    main()
