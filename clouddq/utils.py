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

"""todo: add utils docstring."""
import hashlib
from inspect import getsourcefile
import json
import os
from pathlib import Path
import re
import typing

from dbt.main import main as dbt


ENV_VAR_PATTERN = re.compile(r".*env_var\((.+?)\).*", re.IGNORECASE)


def get_source_file_path():
    return Path(getsourcefile(lambda: 0)).resolve()


def get_project_root_path():
    return get_source_file_path().parent.parent.absolute()


def run_dbt(
    dbt_profile_dir: Path,
    configs: typing.Dict,
    environment: str,
    debug: bool = False,
    dry_run: bool = False,
) -> None:
    """

    Args:
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
    if debug:
        debug_commands = command.copy()
        debug_commands[0] = "debug"
        try:
            print(f"\nExecuting dbt command:\n {debug_commands}")
            dbt(debug_commands)
        except SystemExit:
            pass
        print(f"\nExecuting dbt command:\n {command}")
    if not dry_run:
        dbt(command)


def assert_not_none_or_empty(value: typing.Any, error_msg: str) -> None:
    """

    Args:
      value: typing.Any:
      error_msg: str:

    Returns:

    """
    if not value:
        raise ValueError(error_msg)


def get_from_dict_and_assert(
    config_id: str,
    kwargs: typing.Dict,
    key: str,
    assertion: typing.Callable[[typing.Any], bool] = None,
    error_msg: str = None,
) -> typing.Any:
    value = kwargs.get(key, None)
    assert_not_none_or_empty(
        value, f"Config ID: {config_id} must define non-empty value: '{key}'."
    )
    if assertion and not assertion(value):
        raise ValueError(
            f"Assertion failed on value {value}.\n"
            f"Config ID: {config_id}, kwargs: {kwargs}.\n"
            f"Error: {error_msg}"
        )
    return value


def strip_margin(text: str) -> str:
    """

    Args:
      text: str:

    Returns:

    """

    return re.sub(r"\n[ \t]*\|", "\n", text.strip().lstrip("|"))


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
