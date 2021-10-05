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
import logging
import logging.config
import sys
import traceback

from datetime import datetime
from pathlib import Path
from pprint import pformat
from typing import Optional

import click
import coloredlogs

from git import GitCommandError
from git import InvalidGitRepositoryError
from git import Repo

from clouddq import lib
from clouddq.classes.bigquery_client import BigQueryClient
from clouddq.runners.dbt.dbt_runner import DbtRunner
from clouddq.runners.dbt.dbt_utils import JobStatus
from clouddq.runners.dbt.dbt_utils import get_bigquery_dq_summary_table_name
from clouddq.utils import assert_not_none_or_empty


APP_VERSION = None
try:
    repo = Repo(search_parent_directories=True)
    APP_VERSION = repo.git.describe()
except (InvalidGitRepositoryError, GitCommandError):
    pass

APP_NAME = "clouddq"
LOG_LEVEL = logging._nameToLevel["DEBUG"]


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


class JSONFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record):
        record.msg = json.dumps(
            {
                "severity": record.levelname,
                "time": datetime.utcfromtimestamp(record.created)
                .astimezone()
                .isoformat()
                .replace("+00:00", "Z"),
                "logging.googleapis.com/sourceLocation": {
                    "file": record.pathname or record.filename,
                    "function": record.funcName,
                    "line": record.lineno,
                },
                "exception": record.exc_info,
                "traceback": traceback.format_exception(*record.exc_info)
                if record.exc_info
                else None,
                "message": record.getMessage(),
                "logging.googleapis.com/labels": {
                    "name": APP_NAME,
                    "releaseId": APP_VERSION,
                },
            },
            cls=JsonEncoderDatetime,
        )
        return super().format(record)


def get_json_logger():
    json_logger = logging.getLogger("clouddq-json-logger")
    json_logger.setLevel(LOG_LEVEL)
    logging_stream_handler = logging.StreamHandler(sys.stderr)
    logging_stream_handler.setFormatter(JSONFormatter())
    json_logger.addHandler(logging_stream_handler)
    return json_logger


def get_logger():
    logger = logging.getLogger("clouddq")
    logger.setLevel(LOG_LEVEL)
    logging_stream_handler = logging.StreamHandler(sys.stderr)
    stream_formatter = logging.Formatter(
        "{asctime} {name} {levelname:8s} {message}", style="{"
    )
    logging_stream_handler.setFormatter(stream_formatter)
    logger.addHandler(logging_stream_handler)
    return logger


json_logger = get_json_logger()
logger = get_logger()
coloredlogs.install(logger=logger)


@click.command()
@click.argument("rule_binding_ids")
@click.argument(
    "rule_binding_config_path",
    type=click.Path(exists=True),
)
@click.option(
    "--gcp_project_id",
    help="GCP Project ID used for executing GCP Jobs. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    default=None,
    type=str,
)
@click.option(
    "--gcp_region_id",
    help="GCP region used for running BigQuery Jobs and for storing "
    " any intemediate DQ summary results. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    default=None,
    type=str,
)
@click.option(
    "--gcp_bq_dataset_id",
    help="GCP BigQuery Dataset ID used for storing rule_binding views "
    "and intermediate DQ summary results. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    default=None,
    type=str,
)
@click.option(
    "--gcp_service_account_key_path",
    help="File system path to the exported GCP service account JSON key "
    "for authenticating to GCP. "
    "If --gcp_service_account_key_path is not specified, "
    "defaults to using automatically discovered credentials "
    "via Application Default Credentials (ADC). "
    "More on how ADC discovers credentials: https://google.aip.dev/auth/4110. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    default=None,
    type=click.Path(exists=True),
)
@click.option(
    "--gcp_impersonation_credentials",
    help="Target Service Account Name for authenticating to GCP using "
    "service account impersonation via a source credentials. "
    "Source credentials can be obtained from either "
    "--gcp_service_account_key_path or local ADC credentials. "
    "Ensure the source credentials has sufficient IAM permission to "
    "impersonate the target service account. "
    "This argument will be ignored if --dbt_profiles_dir is set.",
    default=None,
    type=str,
)
@click.option(
    "--dbt_profiles_dir",
    help="Path containing the dbt profiles.yml configs for connecting to GCP. "
    "As dbt supports multiple connection configs in a single profiles.yml file, "
    "you can also specify the dbt target for the run with --environment_target. "
    "Defaults to environment variable DBT_PROFILES_DIR if present. "
    "If another connection config with pattern --*_connection_configs "
    "was not provided, this argument is mandatory. "
    "If --dbt_profiles_dir is present, all other connection configs "
    "with pattern --gcp_* will be ignored. "
    "Passing in dbt configs directly via --dbt_profiles_dir will be "
    "deprecated in v0.1.0. Please migrate to use native-flags for "
    "specifying connection configs instead.",
    type=click.Path(exists=True),
    envvar="DBT_PROFILES_DIR",
)
@click.option(
    "--dbt_path",
    help="Path to dbt model directory where a new view will be created "
    "containing the sql execution statement for each rule binding. "
    "If not specified, clouddq will created a new directory in "
    "the current working directory for the dbt generated sql files. "
    "Passing in dbt models directly via --dbt_path will be "
    "deprecated in v0.1.0. If you will be affected by this "
    "deprecation, please raise a Github issue with details "
    "of your use-case.",
    type=click.Path(exists=True),
    default=None,
)
@click.option(
    "--environment_target",
    help="Execution environment target as defined in dbt profiles.yml, "
    "e.g. dev, test, prod.  "
    "Defaults to 'dev' if not set. "
    "Uses the environment variable ENV if present. "
    "Set this to the same value as 'environment' in "
    "entity 'environment_override' config to trigger "
    "field substitution.",
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
    dbt_path: Optional[str],
    dbt_profiles_dir: Optional[str],
    environment_target: Optional[str],
    gcp_project_id: Optional[str],
    gcp_region_id: Optional[str],
    gcp_bq_dataset_id: Optional[str],
    gcp_service_account_key_path: Optional[Path],
    gcp_impersonation_credentials: Optional[str],
    metadata: Optional[str],
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

    \b
    > python clouddq_executable.zip \\
      T2_DQ_1_EMAIL \\
      configs/ \\
      --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \\
      --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \\
      --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \\
      --metadata='{"key":"value"}' \\

    \b
    > python clouddq_executable.zip \\
      ALL \\
      configs/ \\
      --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \\
      --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \\
      --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \\
      --dry_run  \\
      --debug

    """
    if debug:
        logger.setLevel("DEBUG")
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
            logger.debug("Debug logging enabled")
    logger.info("Starting CloudDQ run with configs:")
    json_logger.warn({"run_configs": locals()})
    if dbt_path:
        logger.warn(
            "Passing in dbt models directly via --dbt_path will be "
            "deprecated in v0.1.0."
        )
    if dbt_profiles_dir:
        logger.warn(
            "If --dbt_profiles_dir is present, all other connection configs "
            "with pattern --gcp_* will be ignored. "
            "Passing in dbt configs directly via --dbt_profiles_dir will be "
            "deprecated in v0.1.0. Please migrate to use native-flags for "
            "specifying connection configs instead."
        )
    bigquery_client = None
    try:
        if not skip_sql_validation:
            # Create BigQuery client for query dry-runs
            bigquery_client = BigQueryClient(
                project_id=gcp_project_id,
                gcp_service_account_key_path=gcp_service_account_key_path,
                gcp_impersonation_credentials=gcp_impersonation_credentials,
            )
        # Prepare dbt runtime
        dbt_runner = DbtRunner(
            dbt_path=dbt_path,
            dbt_profiles_dir=dbt_profiles_dir,
            environment_target=environment_target,
            gcp_project_id=gcp_project_id,
            gcp_region_id=gcp_region_id,
            gcp_bq_dataset_id=gcp_bq_dataset_id,
            gcp_service_account_key_path=gcp_service_account_key_path,
            gcp_impersonation_credentials=gcp_impersonation_credentials,
        )
        dbt_path = dbt_runner.get_dbt_path()
        dbt_rule_binding_views_path = dbt_runner.get_rule_binding_view_path()
        dbt_profiles_dir = dbt_runner.get_dbt_profiles_dir()
        environment_target = dbt_runner.get_dbt_environment_target()
        # Prepare DQ Summary Table
        dq_summary_table_name = get_bigquery_dq_summary_table_name(
            dbt_path=Path(dbt_path),
            dbt_profiles_dir=Path(dbt_profiles_dir),
            environment_target=environment_target,
        )
        if gcp_region_id and not skip_sql_validation:
            dq_summary_dataset = ".".join(dq_summary_table_name.split(".")[:2])
            logger.debug(f"dq_summary_dataset: {dq_summary_dataset}")
            bigquery_client.assert_dataset_is_in_region(
                dataset=dq_summary_dataset, region=gcp_region_id
            )
        logger.info(
            f"Writing summary results to GCP table: `{dq_summary_table_name}`. "
        )
        # Load metadata
        metadata = json.loads(metadata)
        # Load Rule Bindings
        configs_path = Path(rule_binding_config_path)
        logger.debug("Loading rule bindings from: %s", configs_path.absolute())
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
                    f"Creating sql string from configs for rule binding: "
                    f"{rule_binding_id}"
                )
                logger.debug("Rule binding config json:")
                logger.debug(pformat(rule_binding_configs))
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
                bigquery_client.check_query_dry_run(query_string=sql_string)
            logger.debug(
                f"*** Writing sql to {dbt_rule_binding_views_path.absolute()}/"
                f"{rule_binding_id}.sql",
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
        job_status: JobStatus = dbt_runner.run(
            configs=configs,
            debug=debug,
            dry_run=dry_run,
        )
        if job_status == JobStatus.SUCCESS:
            logger.info("Job completed successfully.")
        elif job_status == JobStatus.FAILED:
            raise RuntimeError("Job failed.")
        else:
            raise RuntimeError("Job failed with unknown status.")
    except Exception as error:
        json_logger.error(error, exc_info=True)
        logger.fatal(error)
        raise
    finally:
        if bigquery_client:
            bigquery_client.close_connection()


if __name__ == "__main__":
    main()
