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

from pathlib import Path
from pprint import pformat
from typing import Dict
from typing import Optional

import json
import logging
import os
import re

from dbt.main import main as dbt

from clouddq.utils import assert_not_none_or_empty
from clouddq.utils import load_yaml
from clouddq.utils import working_directory


logger = logging.getLogger(__name__)

ENV_VAR_PATTERN = re.compile(r".*env_var\((.+?)\).*", re.IGNORECASE)


def run_dbt(
    dbt_path: Path,
    dbt_profile_dir: Path,
    configs: Optional[Dict] = None,
    environment: str = "clouddq",
    debug: bool = False,
    dry_run: bool = False,
) -> None:
    """

    Args:
      dbt_path: Path: Path of dbt project described in `dbt_project.yml`
      dbt_profile_dir: str:
      configs: typing.Dict:
      environment: str:
      debug: bool:  (Default value = False)
      dry_run: bool:  (Default value = False)

    Returns:

    """
    if not configs:
        configs = {}
    command = []
    command.extend(["run"])
    command += [
        "--profiles-dir",
        str(dbt_profile_dir),
        "--vars",
        json.dumps(configs),
        "--target",
        environment,
    ]
    try:
        with working_directory(dbt_path):
            if debug:
                logger.debug("Using dbt working directory: %s", Path.cwd())
                debug_commands = command.copy()
                debug_commands[0] = "debug"
                try:
                    logger.info("\nExecuting dbt command:\n %s", debug_commands)
                    dbt(debug_commands)
                except SystemExit:
                    pass
            else:
                if not dry_run:
                    logger.info("\nExecuting dbt command:\n %s", command)
                    dbt(command)
                else:
                    logger.info("\ndbt command generated as part of dry-run:\n %s", command)
    except SystemExit as sysexit:
        if sysexit.code == 0:
            logger.debug("dbt run completed successfully.")
        else:
            raise RuntimeError(
                "dbt run failed with Runtime Error. "
                "See Runtime Error description in dbt run logs for details. "
            )
    except Exception as e:
        raise RuntimeError(f"dbt run failed with unknown error: '{e}'")


def extract_dbt_env_var(text: str) -> str:
    var_env = re.search(ENV_VAR_PATTERN, text).group(1)
    var_env = [x.strip().strip("'\"") for x in var_env.split(",")]
    enrironment_variable = var_env.pop(0)
    default_value = None
    if var_env:
        default_value = var_env.pop()
    value = os.environ.get(enrironment_variable, default_value)
    assert_not_none_or_empty(
        value, f"Enviromment variable not found for dbt env_var variable: {text}"
    )
    return value


def get_bigquery_dq_summary_table_name(
    dbt_path: Path,
    dbt_profiles_dir: Path,
    environment_target: str,
    spark_runner: bool,
    table_name: str = "dq_summary",
) -> str:
    # Get bigquery project and dataset for dq_summary table names
    dbt_project_path = dbt_path.joinpath("dbt_project.yml")
    if not dbt_project_path.is_file():
        raise ValueError(
            "Not able to find 'dbt_project.yml' config file at "
            "input path {dbt_project_path}."
        )
    dbt_profiles_key = load_yaml(dbt_project_path, "profile")
    print("dbt_profiles_key", dbt_profiles_key)
    dbt_profiles_config = load_yaml(dbt_profiles_dir / "profiles.yml", dbt_profiles_key)
    logger.debug(f"Content of dbt_profiles.yml:\n {pformat(dbt_profiles_config)}")
    if not spark_runner:
        dbt_profile = dbt_profiles_config["outputs"][environment_target]
        dbt_project = dbt_profile["project"]
        if "{{" in dbt_project:
            dbt_project = extract_dbt_env_var(dbt_project)
        dbt_dataset = dbt_profile["dataset"]
        if "{{" in dbt_dataset:
            dbt_dataset = extract_dbt_env_var(dbt_dataset)
        dq_summary_table_name = f"{dbt_project}.{dbt_dataset}.{table_name}"
    else:
        spark_dbt_profiles_config = load_yaml(dbt_profiles_dir / "spark_profiles.yml", dbt_profiles_key)
        logger.debug(f"Content of dbt_profiles.yml:\n {pformat(spark_dbt_profiles_config)}")
        spark_dbt_profile = spark_dbt_profiles_config["outputs"][environment_target]
        print(spark_dbt_profile)
        dq_summary_table_name = f"{spark_dbt_profile['schema']}.{table_name}"

    return dq_summary_table_name


def get_dbt_invocation_id(dbt_path: Path) -> str:

    try:
        logger.debug(f"Attempting to get dbt invocation_id from path {dbt_path}")
        # JSON file
        with open(dbt_path.joinpath("target", "manifest.json")) as f:
            # Reading from file
            data = json.loads(f.read())
            return data["metadata"]["invocation_id"]
    except (FileNotFoundError, KeyError) as e:
        logger.error(
            f"dbt invocation_id is not found, dbt hasn't run yet or "
            f"the execution has failed {e}\n",
            exc_info=True,
        )
