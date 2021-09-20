import json
import os
import re
from pathlib import Path
from typing import Dict

import logging
logger = logging.getLogger(__name__)

from dbt.main import main as dbt

from clouddq.utils import working_directory, assert_not_none_or_empty

ENV_VAR_PATTERN = re.compile(r".*env_var\((.+?)\).*", re.IGNORECASE)


def run_dbt(
    dbt_path: Path,
    dbt_profile_dir: Path,
    configs: Dict,
    environment: str,
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
    with working_directory(dbt_path):
        if debug:
            logger.info("Using dbt working directory: %s", Path.cwd())
            debug_commands = command.copy()
            debug_commands[0] = "debug"
            try:
                logger.info("\nExecuting dbt command:\n %s", debug_commands)
                dbt(debug_commands)
            except SystemExit:
                pass
            logger.info("\nExecuting dbt command:\n %s", command)
        if not dry_run:
            dbt(command)

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
