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
from datetime import datetime
from datetime import timezone
from pathlib import Path
from pprint import pformat
from typing import Optional

import json
import logging
import logging.config

import click
import coloredlogs

from clouddq import lib
from clouddq.classes.metadata_registry_defaults import MetadataRegistryDefaults
from clouddq.integration.bigquery.bigquery_client import BigQueryClient
from clouddq.integration.bigquery.dq_target_table_utils import TargetTable
from clouddq.integration.dataplex.clouddq_dataplex import CloudDqDataplexClient
from clouddq.integration.gcp_credentials import GcpCredentials
from clouddq.log import JsonEncoderDatetime
from clouddq.log import add_cloud_logging_handler
from clouddq.log import get_json_logger
from clouddq.log import get_logger
from clouddq.runners.dbt.dbt_runner import DbtRunner
from clouddq.runners.dbt.dbt_utils import get_bigquery_dq_summary_table_name
from clouddq.runners.dbt.dbt_utils import get_dbt_invocation_id
from clouddq.runners.dbt.dbt_utils import get_spark_dq_summary_table_name
from clouddq.utils import assert_not_none_or_empty


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
    "--target_bigquery_summary_table",
    help="Target Bigquery summary table for data quality output. "
    "This should be a fully qualified table name in the format"
    "<project_id>.<dataset_id>.<table_name>",
    type=str,
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
    "deprecated in v1.0.0. Please migrate to use native-flags for "
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
    "deprecated in v1.0.0. If you will be affected by this "
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
@click.option(
    "--summary_to_stdout",
    help="If True, the summary of the validation results will be logged to stdout. "
    "This flag only takes effect if target_bigquery_summary_table is specified as well.",
    is_flag=True,
    default=False,
)
@click.option(
    "--enable_experimental_bigquery_entity_uris",
    help="If True, allows looking up entity_uris with scheme 'bigquery://' "
    "using Dataplex Metadata API. ",
    is_flag=True,
    default=False,
)
@click.option(
    "--enable_experimental_pyspark_runner",
    help="If True, allows running pyspark clouddq job",
    is_flag=True,
    default=False,
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
    target_bigquery_summary_table: Optional[str],
    debug: bool = False,
    print_sql_queries: bool = False,
    skip_sql_validation: bool = False,
    summary_to_stdout: bool = False,
    enable_experimental_bigquery_entity_uris: bool = False,
    enable_experimental_pyspark_runner: bool = False,
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
      --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}" \\
      --metadata='{"key":"value"}' \\

    \b
    > python clouddq_executable.zip \\
      ALL \\
      configs/ \\
      --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \\
      --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \\
      --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \\
      --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}" \\
      --dry_run  \\
      --debug

    """
    if debug:
        logger.setLevel("DEBUG")
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
            logger.debug("Debug logging enabled")
    if dbt_path:
        logger.warning(
            "Passing in dbt models directly via --dbt_path will be "
            "deprecated in v1.0.0"
        )
    if dbt_profiles_dir:
        logger.warning(
            "If --dbt_profiles_dir is present, all other connection configs "
            "with pattern --gcp_* will be ignored. "
            "Passing in dbt configs directly via --dbt_profiles_dir will be "
            "deprecated in v1.0.0. Please migrate to use native-flags for "
            "specifying connection configs instead."
        )
    if (
        not dbt_profiles_dir
        and (  # noqa: W503
            not gcp_project_id or not gcp_bq_dataset_id or not gcp_region_id
        )
    ) or (dbt_profiles_dir and (gcp_project_id or gcp_bq_dataset_id or gcp_region_id)):
        raise ValueError(
            "CLI input must define connection configs using '--dbt_profiles_dir' or "
            "using '--gcp_project_id', '--gcp_bq_dataset_id', and '--gcp_region_id'."
        )
    bigquery_client = None
    try:
        gcp_credentials = GcpCredentials(
            gcp_project_id=gcp_project_id,
            gcp_service_account_key_path=gcp_service_account_key_path,
            gcp_impersonation_credentials=gcp_impersonation_credentials,
        )
        # Set-up cloud logging
        add_cloud_logging_handler(logger=json_logger)
        logger.info("Starting CloudDQ run with configs:")
        json_logger.warning(
            json.dumps({"clouddq_run_configs": locals()}, cls=JsonEncoderDatetime)
        )
        if not skip_sql_validation:
            # Create BigQuery client for query dry-runs
            bigquery_client = BigQueryClient(gcp_credentials=gcp_credentials)
        # Load metadata
        metadata = json.loads(metadata)
        # Load Rule Bindings
        configs_path = Path(rule_binding_config_path)
        logger.debug(f"Loading rule bindings from: {configs_path.absolute()}")
        all_rule_bindings = lib.load_rule_bindings_config(Path(configs_path))
        # Prepare list of Rule Bindings in-scope for run
        target_rule_binding_ids = [r.strip() for r in rule_binding_ids.split(",")]
        if len(target_rule_binding_ids) == 1 and target_rule_binding_ids[0] == "ALL":
            target_rule_binding_ids = list(all_rule_bindings.keys())
        logger.info(f"Preparing SQL for rule bindings: {target_rule_binding_ids}")
        # Load default configs for metadata registries
        registry_defaults: MetadataRegistryDefaults = (
            lib.load_metadata_registry_default_configs(Path(configs_path))
        )
        default_dataplex_projects = registry_defaults.get_dataplex_registry_defaults(
            "projects"
        )
        default_dataplex_locations = registry_defaults.get_dataplex_registry_defaults(
            "locations"
        )
        default_dataplex_lakes = registry_defaults.get_dataplex_registry_defaults(
            "lakes"
        )
        dataplex_registry_defaults = registry_defaults.get_dataplex_registry_defaults()
        # Prepare Dataplex Client from metadata registry defaults
        dataplex_client = CloudDqDataplexClient(
            gcp_credentials=gcp_credentials,
            gcp_project_id=default_dataplex_projects,
            gcp_dataplex_lake_name=default_dataplex_lakes,
            gcp_dataplex_region=default_dataplex_locations,
        )
        logger.debug(
            "Created CloudDqDataplexClient with arguments: "
            f"{gcp_credentials}, "
            f"{default_dataplex_projects}, "
            f"{default_dataplex_lakes}, "
            f"{default_dataplex_locations}, "
        )
        # Load all configs into a local cache
        configs_cache = lib.prepare_configs_cache(configs_path=Path(configs_path))
        configs_cache.resolve_dataplex_entity_uris(
            client=dataplex_client,
            default_configs=dataplex_registry_defaults,
            target_rule_binding_ids=target_rule_binding_ids,
            enable_experimental_bigquery_entity_uris=enable_experimental_bigquery_entity_uris,
        )

        logger.debug("Target BQ rule bindings")
        bq_rule_bindings = configs_cache.get_bq_rule_bindings()
        bq_target_rule_bindings = []
        for bq_rule_binding in bq_rule_bindings:
            logger.debug(bq_rule_binding)
            if bq_rule_binding in target_rule_binding_ids:
                bq_target_rule_bindings.append(bq_rule_binding)

        spark_runner = False
        spark_dbt_runner = None
        if enable_experimental_pyspark_runner:
            logger.debug("Target Spark rule bindings")
            spark_rule_bindings = configs_cache.get_spark_rule_bindings()
            spark_target_rule_bindings = []
            for spark_rule_binding in spark_rule_bindings:
                logger.debug(spark_rule_binding)
                if spark_rule_binding in target_rule_binding_ids:
                    spark_target_rule_bindings.append(spark_rule_binding)

            if spark_target_rule_bindings:
                spark_runner = True
                # Prepare spark dbt runtime
                spark_dbt_runner = DbtRunner(
                    dbt_path=dbt_path,
                    dbt_profiles_dir=dbt_profiles_dir,
                    environment_target="spark_thrift",
                    gcp_project_id=gcp_project_id,
                    gcp_region_id=gcp_region_id,
                    gcp_bq_dataset_id=gcp_bq_dataset_id,
                    gcp_service_account_key_path=gcp_service_account_key_path,
                    gcp_impersonation_credentials=gcp_impersonation_credentials,
                    spark_runner=spark_runner,
                )

                print("Spark DBT Runner")
                print(spark_dbt_runner.connection_config)
                spark_dbt_path = spark_dbt_runner.get_dbt_path(spark_runner=True)
                print("Spark DBT path is", spark_dbt_path)
                spark_dbt_rule_binding_views_path = (
                    spark_dbt_runner.get_rule_binding_view_path()
                )
                print(
                    "Spark DBT rule binding views  path is",
                    spark_dbt_rule_binding_views_path,
                )
                spark_dbt_profiles_dir = spark_dbt_runner.get_dbt_profiles_dir()
                print("Spark DBT profiles directory", spark_dbt_profiles_dir)
                spark_environment_target = spark_dbt_runner.get_dbt_environment_target()

        # Prepare dbt runtime
        bq_dbt_runner = DbtRunner(
            dbt_path=dbt_path,
            dbt_profiles_dir=dbt_profiles_dir,
            environment_target=environment_target,
            gcp_project_id=gcp_project_id,
            gcp_region_id=gcp_region_id,
            gcp_bq_dataset_id=gcp_bq_dataset_id,
            gcp_service_account_key_path=gcp_service_account_key_path,
            gcp_impersonation_credentials=gcp_impersonation_credentials,
        )
        print("BQ DBT Runner")
        print(bq_dbt_runner.connection_config)
        bq_dbt_path = bq_dbt_runner.get_dbt_path(spark_runner=False)
        print("DBT path is", bq_dbt_path)
        bq_dbt_rule_binding_views_path = bq_dbt_runner.get_rule_binding_view_path()
        print("DBT rule binding views  path is", bq_dbt_rule_binding_views_path)
        bq_dbt_profiles_dir = bq_dbt_runner.get_dbt_profiles_dir()
        print("DBT profiles directory", bq_dbt_profiles_dir)
        bq_environment_target = bq_dbt_runner.get_dbt_environment_target()

        # Prepare DQ Summary Table
        bq_dq_summary_table_name = get_bigquery_dq_summary_table_name(
            dbt_path=Path(bq_dbt_path),
            dbt_profiles_dir=Path(bq_dbt_profiles_dir),
            environment_target=bq_environment_target,
        )

        logger.info(
            "Writing BigQuery rule_binding views and intermediate "
            f"summary results to BigQuery table: `{bq_dq_summary_table_name}`. "
        )
        if enable_experimental_pyspark_runner:
            if spark_runner:
                # Prepare Spark DQ Summary Table
                spark_dq_summary_table_name = get_spark_dq_summary_table_name(
                    dbt_path=Path(spark_dbt_path),
                    dbt_profiles_dir=Path(spark_dbt_profiles_dir),
                    environment_target=spark_environment_target,
                )
                logger.info(
                    f"Preparing Spark DQ Summary table: `{spark_dq_summary_table_name}`. "
                )
        if not spark_runner:
            dq_summary_table_exists = False
            if gcp_region_id and not skip_sql_validation:
                dq_summary_dataset = ".".join(bq_dq_summary_table_name.split(".")[:2])
                logger.debug(f"dq_summary_dataset: {dq_summary_dataset}")
                bigquery_client.assert_dataset_is_in_region(
                    dataset=dq_summary_dataset, region=gcp_region_id
                )
                dq_summary_table_exists = bigquery_client.is_table_exists(
                    bq_dq_summary_table_name
                )
                bigquery_client.assert_required_columns_exist_in_table(
                    bq_dq_summary_table_name
                )
        # Check existence of dataset for target BQ table in the selected GCP region
        if target_bigquery_summary_table:
            logger.info(
                "Writing summary results to target BigQuery table: "
                f"`{target_bigquery_summary_table}`. "
            )
            target_table_ref = bigquery_client.table_from_string(
                target_bigquery_summary_table
            )
            target_dataset_id = target_table_ref.dataset_id
            logger.debug(
                f"BigQuery dataset used in --target_bigquery_summary_table: {target_dataset_id}"
            )
            if not bigquery_client.is_dataset_exists(target_dataset_id):
                raise AssertionError(
                    "Invalid argument to --target_bigquery_summary_table: "
                    f"{target_bigquery_summary_table}. "
                    f"Dataset {target_dataset_id} does not exist. "
                )
            bigquery_client.assert_dataset_is_in_region(
                dataset=target_dataset_id, region=gcp_region_id
            )
            bigquery_client.assert_required_columns_exist_in_table(
                target_bigquery_summary_table
            )
        else:
            logger.warning(
                "CLI --target_bigquery_summary_table is not set. This will become a required argument in v1.0.0."
            )
        if summary_to_stdout and target_bigquery_summary_table:
            logger.info(
                "--summary_to_stdout is True. Logging summary results as json to stdout."
            )
        elif summary_to_stdout and not target_bigquery_summary_table:
            logger.warning(
                "--summary_to_stdout is True but --target_bigquery_summary_table is not set. "
                "No summary logs will be logged to stdout."
            )
        for rule_binding_id in bq_target_rule_bindings:
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
                logger.debug(
                    f"Rule binding config json:\n{pformat(rule_binding_configs)}"
                )
            sql_string = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name=bq_dq_summary_table_name,
                configs_cache=configs_cache,
                environment=bq_environment_target,
                metadata=metadata,
                debug=print_sql_queries,
                progress_watermark=progress_watermark,
                default_configs=dataplex_registry_defaults,
                dq_summary_table_exists=dq_summary_table_exists,
            )
            if not skip_sql_validation:
                logger.debug(
                    f"Validating generated SQL code for rule binding "
                    f"{rule_binding_id} using BigQuery dry-run client.",
                )
                bigquery_client.check_query_dry_run(query_string=sql_string)
            logger.debug(
                f"*** Writing sql to {bq_dbt_rule_binding_views_path.absolute()}/"
                f"{rule_binding_id}.sql",
            )
            lib.write_sql_string_as_dbt_model(
                rule_binding_id=rule_binding_id,
                sql_string=sql_string,
                dbt_rule_binding_views_path=bq_dbt_rule_binding_views_path,
            )
            # clean up old bq rule_bindings
            for view in bq_dbt_rule_binding_views_path.glob("*.sql"):
                if view.stem not in bq_target_rule_bindings:
                    view.unlink()

        if enable_experimental_pyspark_runner:
            for rule_binding_id in spark_target_rule_bindings:
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
                    logger.debug(
                        f"Rule binding config json:\n{pformat(rule_binding_configs)}"
                    )
                sql_string = lib.create_rule_binding_view_model(
                    rule_binding_id=rule_binding_id,
                    rule_binding_configs=rule_binding_configs,
                    dq_summary_table_name=spark_dq_summary_table_name,
                    configs_cache=configs_cache,
                    environment=spark_environment_target,
                    metadata=metadata,
                    debug=print_sql_queries,
                    progress_watermark=progress_watermark,
                    default_configs=dataplex_registry_defaults,
                    spark_runner=spark_runner,
                )
                logger.debug(
                    f"*** Writing sql to {spark_dbt_rule_binding_views_path.absolute()}/"
                    f"{rule_binding_id}.sql",
                )
                lib.write_sql_string_as_dbt_model(
                    rule_binding_id=rule_binding_id,
                    sql_string=sql_string,
                    dbt_rule_binding_views_path=spark_dbt_rule_binding_views_path,
                )
                # clean up old spark rule_bindings
                for view in spark_dbt_rule_binding_views_path.glob("*.sql"):
                    if view.stem not in spark_target_rule_bindings:
                        view.unlink()

        # create dbt configs json for the main.sql loop and run dbt
        if bq_target_rule_bindings:
            bq_configs = {"target_rule_binding_ids": bq_target_rule_bindings}
            bq_dbt_runner.run_bq(
                configs=bq_configs,
                debug=debug,
                dry_run=dry_run,
            )
        if enable_experimental_pyspark_runner:
            if spark_target_rule_bindings:
                # create dbt configs json for the main.sql loop and run dbt
                spark_configs = {"target_rule_binding_ids": spark_target_rule_bindings}
                spark_dbt_runner.run_spark(
                    configs=spark_configs,
                    debug=debug,
                    dry_run=dry_run,
                )

        if not dry_run:
            if target_bigquery_summary_table:
                if not spark_runner:
                    invocation_id = get_dbt_invocation_id(bq_dbt_path)
                    logger.info(
                        f"bq dbt invocation id for current execution "
                        f"is {invocation_id}"
                    )
                    partition_date = datetime.now(timezone.utc).date()
                    target_table = TargetTable(invocation_id, bigquery_client)
                    num_rows = target_table.write_to_target_bq_table(
                        partition_date,
                        target_bigquery_summary_table,
                        bq_dq_summary_table_name,
                        summary_to_stdout,
                    )
                    json_logger.info(
                        json.dumps(
                            {
                                "clouddq_job_completion_config": {
                                    "invocation_id": invocation_id,
                                    "target_bigquery_summary_table": target_bigquery_summary_table,
                                    "summary_to_stdout": summary_to_stdout,
                                    "target_rule_binding_ids": bq_target_rule_bindings,
                                    "partition_date": partition_date,
                                    "num_rows_loaded_to_target_table": num_rows,
                                }
                            },
                            cls=JsonEncoderDatetime,
                        )
                    )
                    logger.info("Job completed successfully.")
                else:
                    invocation_id = get_dbt_invocation_id(spark_dbt_path)
                    logger.info(
                        f"spark dbt invocation id for current execution "
                        f"is {invocation_id}"
                    )
                    partition_date = datetime.now(timezone.utc).date()
                    logger.info(f"Partition date is {partition_date}")
                    target_table = TargetTable(invocation_id, bigquery_client)
                    num_rows = target_table.write_to_target_bq_table(
                        partition_date,
                        target_bigquery_summary_table,
                        spark_dq_summary_table_name,
                        summary_to_stdout,
                        spark_runner=spark_runner,
                    )
                    json_logger.info(
                        json.dumps(
                            {
                                "clouddq_job_completion_config": {
                                    "invocation_id": invocation_id,
                                    "target_bigquery_summary_table": target_bigquery_summary_table,
                                    "summary_to_stdout": summary_to_stdout,
                                    "target_rule_binding_ids": spark_target_rule_bindings,
                                    "partition_date": partition_date,
                                    "num_rows_loaded_to_target_table": num_rows,
                                }
                            },
                            cls=JsonEncoderDatetime,
                        )
                    )
                    logger.info("Job completed successfully.")
            else:
                logger.warning(
                    "'--target_bigquery_summary_table' was not provided. "
                    "It is needed to append the dq summary results to the "
                    "provided target bigquery table. This will become a "
                    "required argument in v1.0.0"
                )
                if summary_to_stdout:
                    logger.warning(
                        "'--summary_to_stdout' was set but does"
                        " not take effect unless "
                        "'--target_bigquery_summary_table' is provided"
                    )
    except Exception as error:
        logger.error(error, exc_info=True)
        json_logger.error(error, exc_info=True)
        raise SystemExit(f"\n\n{error}")
    finally:
        if bigquery_client:
            bigquery_client.close_connection()


if __name__ == "__main__":
    main()
