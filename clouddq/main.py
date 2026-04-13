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
    help="GCP Project ID used for executing GCP Jobs. ",
    default=None,
    type=str,
)
@click.option(
    "--gcp_region_id",
    help="GCP region used for running BigQuery Jobs and for storing "
    " any intermediate DQ summary results. This is an optional argument. "
    "It will default to the region of the --gcp_bq_dataset_id if not provided.",
    default=None,
    type=str,
)
@click.option(
    "--gcp_bq_dataset_id",
    help="GCP BigQuery Dataset ID used for storing rule_binding views "
    "and intermediate DQ summary results. This dataset must be located "
    "in project --gcp_project_id and region --gcp_region_id."
    "If --gcp_region_id is not provided, BigQuery jobs will be created "
    "in the same GCP region as this dataset. ",
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
    "More on how ADC discovers credentials: https://google.aip.dev/auth/4110. ",
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
    "impersonate the target service account. ",
    default=None,
    type=str,
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
    "--enable_experimental_dataplex_gcs_validation",
    help="This flag has no effect and is now deprecated. GCS resource validation via BigQuery "
    "External Tables is supported regardless of the value of this flag. "
    "Please remove it from your script.",
    is_flag=True,
    default=True,
)
@click.option(
    "--enable_experimental_bigquery_entity_uris",
    help="This flag has no effect and is now deprecated. "
    "BigQuery entity URI is supported regardless of the value of this flag. "
    "Please remove it from your script. ",
    is_flag=True,
    default=True,
)
@click.option(
    "--intermediate_table_expiration_hours",
    help="Configure the bigquery job intermediate tables expiration hours "
    "in the provided dataset '--gcp_bq_dataset_id' ",
    default=24,
    type=int,
    show_default=True,
)
@click.option(
    "--num_threads",
    help="Number of concurrent bigquery operations that can be "
    "increased to reduce run-time. We advice setting "
    "this to number of cores of your run-environment machines",
    default=8,
    type=int,
)
def main(  # noqa: C901
    rule_binding_ids: str,
    rule_binding_config_path: str,
    environment_target: Optional[str],
    gcp_project_id: Optional[str],
    gcp_region_id: Optional[str],
    gcp_bq_dataset_id: Optional[str],
    gcp_service_account_key_path: Optional[Path],
    gcp_impersonation_credentials: Optional[str],
    metadata: Optional[str],
    dry_run: bool,
    progress_watermark: bool,
    target_bigquery_summary_table: str,
    intermediate_table_expiration_hours: int,
    num_threads: int,
    debug: bool = False,
    print_sql_queries: bool = False,
    skip_sql_validation: bool = False,
    summary_to_stdout: bool = False,
    enable_experimental_bigquery_entity_uris: bool = True,
    enable_experimental_dataplex_gcs_validation: bool = True,
) -> None:
    """Run RULE_BINDING_IDS from a RULE_BINDING_CONFIG_PATH..

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
      --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}" \\
      --metadata='{"key":"value"}' \\

    \b
    > python clouddq_executable.zip \\
      ALL \\
      configs/ \\
      --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \\
      --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \\
      --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}" \\
      --dry_run  \\
      --debug

    """
    if debug:
        logger.setLevel("DEBUG")
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
            logger.debug("Debug logging enabled")

    if not gcp_project_id or not gcp_bq_dataset_id:
        raise ValueError(
            "CLI input must define connection configs using the parameters: "
            "'--gcp_project_id', '--gcp_bq_dataset_id', '--gcp_region_id')."
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
        # Create BigQuery client
        bigquery_client = BigQueryClient(gcp_credentials=gcp_credentials)
        # Prepare dbt runtime
        dbt_runner = DbtRunner(
            environment_target=environment_target,
            gcp_project_id=gcp_project_id,
            gcp_region_id=gcp_region_id,
            gcp_bq_dataset_id=gcp_bq_dataset_id,
            bigquery_client=bigquery_client,
            gcp_service_account_key_path=gcp_service_account_key_path,
            gcp_impersonation_credentials=gcp_impersonation_credentials,
            intermediate_table_expiration_hours=intermediate_table_expiration_hours,
            num_threads=num_threads,
        )
        dbt_path = dbt_runner.get_dbt_path()
        dbt_rule_binding_views_path = dbt_runner.get_rule_binding_view_path()
        dbt_entity_summary_path = dbt_runner.get_entity_summary_path()

        (
            dbt_profiles_dir,
            environment_target,
        ) = dbt_runner.get_dbt_profiles_dir_and_environment_target(
            gcp_project_id=gcp_project_id,
            gcp_bq_dataset_id=gcp_bq_dataset_id,
            gcp_region_id=gcp_region_id,
            gcp_impersonation_credentials=gcp_impersonation_credentials,
            bigquery_client=bigquery_client,
        )

        # Prepare DQ Summary Table
        dq_summary_table_name = get_bigquery_dq_summary_table_name(
            dbt_path=Path(dbt_path),
            dbt_profiles_dir=Path(dbt_profiles_dir),
            environment_target=environment_target,
        )
        logger.info(
            "Writing rule_binding views and intermediate summary "
            f"results to BigQuery dq_summary_table_name: `{dq_summary_table_name}`. "
        )
        dq_summary_table_exists = False
        dq_summary_table_ref = bigquery_client.table_from_string(dq_summary_table_name)
        dq_summary_dataset = (
            f"{dq_summary_table_ref.project}.{dq_summary_table_ref.dataset_id}"
        )
        logger.info(f"Using dq_summary_dataset: {dq_summary_dataset}")
        dq_summary_table_exists = bigquery_client.is_table_exists(
            table=dq_summary_table_name
        )
        if not bigquery_client.is_dataset_exists(dataset=dq_summary_dataset):
            raise AssertionError(
                "Invalid argument to --gcp_bq_dataset_id: "
                f"Dataset {dq_summary_dataset} does not exist. "
            )
        dq_summary_dataset_region = bigquery_client.get_dataset_region(
            dataset=dq_summary_dataset,
        )
        if gcp_region_id and dq_summary_dataset_region != gcp_region_id:
            raise AssertionError(
                f"GCP region in --gcp_region_id '{gcp_region_id}' "
                f"must be the same as dq_summary_dataset "
                f"'{dq_summary_dataset}' region: "
                f"'{dq_summary_dataset_region}'."
            )
        dq_summary_missing_required_fields = (
            bigquery_client.assert_required_columns_exist_in_table(
                table=dq_summary_table_name
            )
        )
        if dq_summary_missing_required_fields:
            for field in dq_summary_missing_required_fields.values():
                bigquery_client.execute_query(query_string=field).result()
        # Check existence of dataset for target BQ table in the selected GCP region
        if target_bigquery_summary_table:
            logger.info(
                "Using target_bigquery_summary_table: "
                f"`{target_bigquery_summary_table}`. "
            )
            target_table_ref = bigquery_client.table_from_string(
                target_bigquery_summary_table
            )
            target_dataset = f"{target_table_ref.project}.{target_table_ref.dataset_id}"
            logger.debug(
                f"BigQuery dataset used in --target_bigquery_summary_table: "
                f"{target_dataset}"
            )
            if not bigquery_client.is_dataset_exists(dataset=target_dataset):
                raise AssertionError(
                    "Invalid argument to --target_bigquery_summary_table: "
                    f"{target_bigquery_summary_table}. "
                    f"Dataset {target_dataset} does not exist. "
                )
            target_dataset_region = bigquery_client.get_dataset_region(
                dataset=target_dataset
            )
            if gcp_region_id and target_dataset_region != gcp_region_id:
                raise AssertionError(
                    f"GCP region in --gcp_region_id '{gcp_region_id}' "
                    f"must be the same as --target_bigquery_summary_table "
                    f"'{target_dataset}' region "
                    f"'{target_dataset_region}'."
                )
            if target_dataset_region != dq_summary_dataset_region:
                raise ValueError(
                    f"GCP region for --gcp_bq_dataset_id "
                    f"'{dq_summary_project_id}.{dq_summary_dataset}': "
                    f"'{dq_summary_dataset_region}' must be the same as "
                    f"GCP region for --target_bigquery_summary_table "
                    f"'{dq_summary_project_id}.{dq_summary_dataset}': "
                    f"'{target_dataset_region}'"
                )
            target_table_missing_required_fields = (
                bigquery_client.assert_required_columns_exist_in_table(
                    table=target_bigquery_summary_table
                )
            )
            if target_table_missing_required_fields:
                for field in target_table_missing_required_fields.values():
                    bigquery_client.execute_query(query_string=field).result()
        else:
            logger.warning(
                "CLI --target_bigquery_summary_table is not set. This will become a required argument in v1.0.0."
            )
        # Log information about --summary_to_stdout
        if summary_to_stdout and target_bigquery_summary_table:
            logger.info(
                "--summary_to_stdout is True. Logging summary results as json to stdout."
            )
        elif summary_to_stdout and not target_bigquery_summary_table:
            logger.warning(
                "--summary_to_stdout is True but --target_bigquery_summary_table is not set. "
                "No summary logs will be logged to stdout."
            )
        # Load metadata
        metadata = json.loads(metadata)
        # Load Rule Bindings
        configs_path = Path(rule_binding_config_path)
        logger.debug(f"Loading rule bindings from: {configs_path.absolute()}")
        all_rule_bindings = lib.load_rule_bindings_config(Path(configs_path))
        # Prepare list of Rule Bindings in-scope for run
        target_rule_binding_ids = [
            r.strip().upper() for r in rule_binding_ids.split(",")
        ]
        if len(target_rule_binding_ids) == 1 and target_rule_binding_ids[0] == "ALL":
            target_rule_binding_ids = [
                rule_binding.upper() for rule_binding in all_rule_bindings.keys()
            ]
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
            dataplex_client=dataplex_client,
            bigquery_client=bigquery_client,
            default_configs=dataplex_registry_defaults,
            target_rule_binding_ids=target_rule_binding_ids,
        )
        # Get Entities for entity-level summary views
        target_entity_summary_configs: dict = (
            configs_cache.get_entities_configs_from_rule_bindings(
                target_rule_binding_ids=target_rule_binding_ids,
            )
        )
        # Create Rule_binding views
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
                logger.debug(
                    f"Rule binding config json:\n{pformat(rule_binding_configs)}"
                )
            high_watermark_filter_exists = False

            configs = lib.create_rule_binding_view_model(
                rule_binding_id=rule_binding_id,
                rule_binding_configs=rule_binding_configs,
                dq_summary_table_name=dq_summary_table_name,
                configs_cache=configs_cache,
                environment=environment_target,
                metadata=metadata,
                debug=print_sql_queries,
                progress_watermark=progress_watermark,
                default_configs=dataplex_registry_defaults,
                dq_summary_table_exists=dq_summary_table_exists,
                high_watermark_filter_exists=high_watermark_filter_exists,
                bigquery_client=bigquery_client,
            )
            if not skip_sql_validation:
                logger.debug(
                    f"Validating generated SQL code for rule binding "
                    f"{rule_binding_id} using BigQuery dry-run client.",
                )
                bigquery_client.check_query_dry_run(
                    query_string=configs.get("generated_sql_string_dict").get(
                        f"{rule_binding_id}_generated_sql_string"
                    )
                )
            logger.debug(
                f"*** Writing sql to {dbt_rule_binding_views_path.absolute()}/"
                f"{rule_binding_id}.sql",
            )
            lib.write_sql_string_as_dbt_model(
                model_id=rule_binding_id,
                sql_string=configs.get("generated_sql_string_dict").get(
                    f"{rule_binding_id}_generated_sql_string"
                ),
                dbt_model_path=dbt_rule_binding_views_path,
            )

        # clean up old rule_bindings
        for view in dbt_rule_binding_views_path.glob("*.sql"):
            if view.stem.upper() not in target_rule_binding_ids:
                view.unlink()
        logger.info(
            f"target_entity_summary_configs:\n{pformat(target_entity_summary_configs)}"
        )
        # create entity-level summary table models
        for (
            entity_table_id,
            entity_configs_dict,
        ) in target_entity_summary_configs.items():
            rule_binding_ids_list = entity_configs_dict.get("rule_binding_ids_list")
            assert_not_none_or_empty(
                rule_binding_ids_list,
                f"Internal Error: no rule_binding_id found for entity_table_id {entity_table_id}.",
            )
            sql_string = lib.create_entity_summary_model(
                entity_table_id=entity_table_id,
                entity_target_rule_binding_configs=entity_configs_dict,
                gcp_project_id=gcp_project_id,
                gcp_bq_dataset_id=gcp_bq_dataset_id,
                debug=print_sql_queries,
            )
            logger.debug(
                f"*** Writing sql to {dbt_entity_summary_path.absolute()}/"
                f"{entity_table_id}.sql",
            )
            lib.write_sql_string_as_dbt_model(
                model_id=entity_table_id,
                sql_string=sql_string,
                dbt_model_path=dbt_entity_summary_path,
            )
        # clean up old entity_summary views
        for view in dbt_entity_summary_path.glob("*.sql"):
            if view.stem not in target_entity_summary_configs.keys():
                view.unlink()
        # create dbt configs json for the main.sql loop and run dbt
        configs = {
            "entity_dq_statistics_models": list(target_entity_summary_configs.keys()),
        }
        dbt_runner.run(
            configs=configs,
            debug=debug,
            dry_run=dry_run,
        )
        if not dry_run:
            if target_bigquery_summary_table:
                if target_bigquery_summary_table == dq_summary_table_name:
                    raise ValueError(
                        f"The target bigquery summary table name `{target_bigquery_summary_table}` "
                        f"cannot be same as dq summary table name `{dq_summary_table_name}` which "
                        f"is reserved for storing the intermediate results used by clouddq "
                        f"for further processing in case of incremental validation."
                    )
                else:
                    invocation_id = get_dbt_invocation_id(dbt_path)
                    logger.info(
                        f"dbt invocation id for current execution "
                        f"is {invocation_id}"
                    )
                    partition_date = datetime.now(timezone.utc).date()
                    target_table = TargetTable(invocation_id, bigquery_client)
                    num_rows = target_table.write_to_target_bq_table(
                        partition_date,
                        target_bigquery_summary_table,
                        dq_summary_table_name,
                        summary_to_stdout,
                    )
                    json_logger.info(
                        json.dumps(
                            {
                                "clouddq_job_completion_config": {
                                    "invocation_id": invocation_id,
                                    "target_bigquery_summary_table": target_bigquery_summary_table,
                                    "summary_to_stdout": summary_to_stdout,
                                    "target_rule_binding_ids": target_rule_binding_ids,
                                    "partition_date": partition_date,
                                    "num_rows_loaded_to_target_table": num_rows,
                                }
                            },
                            cls=JsonEncoderDatetime,
                        )
                    )
                    logger.info("Job completed successfully.")
            else:
                raise ValueError(
                    "'--target_bigquery_summary_table' was not provided. "
                    "It is needed to append the dq summary results to the "
                    "provided target bigquery table."
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
