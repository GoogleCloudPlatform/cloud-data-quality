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
from inspect import getsourcefile
from pathlib import Path
from pprint import pformat

import contextlib
import hashlib
import json
import logging
import os
import random
import re
import shutil
import string
import time
import typing

from jinja2 import ChainableUndefined  # type: ignore
from jinja2 import DebugUndefined
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape

import yaml


logger = logging.getLogger(__name__)


MAXIMUM_EXPONENTIAL_BACKOFF_SECONDS = 32


def load_yaml(file_path: Path, key: str = None) -> typing.Any:
    with file_path.open() as f:
        yaml_configs = yaml.safe_load(f)
    if not yaml_configs:
        return dict()
    output = yaml_configs.get(key, dict())
    if type(output) == dict:
        return {key.upper(): value for key, value in output.items()}
    elif type(output) == list:
        return [item.upper() for item in output]
    elif type(output) == str:
        return output
    else:
        raise RuntimeError(
            f"CloudDQ configs file `{file_path}` has empty config node type `{key}:`. "
            "Please remove the node if it is not being used."
        )


def unnest_object_to_list(object: dict) -> list:
    collection = []
    for object_id, object_content in object.items():
        collection.append({"id": object_id.upper(), **object_content})
    return collection


def convert_json_value_to_dict(object: dict, key: str):
    if key in object:
        if object[key] is None:
            object[key] = {}
        else:
            object[key] = json.loads(object[key])


def get_templates_path(file_path: Path) -> Path:
    template_path = (
        Path(getsourcefile(lambda: 0)).resolve().parent.joinpath("templates", file_path)
    )
    return template_path


def get_template_file(file_path: Path) -> str:
    template_path = get_templates_path(file_path)
    if not template_path.is_file():
        raise FileNotFoundError(
            f"No clouddq template found for file_path {file_path}"
            f" in path {template_path.absolute()}"
        )
    data = template_path.read_text()
    return data


def write_templated_file_to_path(path: Path, lookup_table: typing.Dict) -> None:
    logger.debug(f"Writing templated file {get_templates_path(path.name)} to {path}")
    path.write_text(get_template_file(lookup_table.get(path.name)))


@contextlib.contextmanager
def working_directory(path):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


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
            f"Assertion failed on value '{value}'.\n"
            f"Config ID: {config_id}, kwargs: {pformat(kwargs)}.\n"
            f"Error: {error_msg}"
        )
    return value


def get_keys_from_dict_and_assert_oneof(
    config_id: str,
    kwargs: typing.Dict,
    keys: typing.List[str],
    assertion: typing.Callable[[typing.Any], bool] = None,
    error_msg: str = None,
) -> typing.Any:
    value = {key: kwargs.get(key) for key in keys if kwargs.get(key, None) is not None}
    if len(value) != 1:
        raise ValueError(
            f"Config ID: {config_id} must define exactly one key from: {keys}."
        )
    if assertion and not assertion(value):
        raise ValueError(
            f"Assertion failed on value '{value}'.\n"
            f"Config ID: {config_id}, kwargs: {kwargs}.\n"
            f"Error: {error_msg}"
        )
    return value


class DebugChainableUndefined(ChainableUndefined, DebugUndefined):
    pass


def load_jinja_template(template_path: Path) -> Template:
    try:
        environment = load_jinja_template.environment
        return environment.get_template(template_path.name)
    except AttributeError:
        templates_parent_path = get_templates_path(template_path.parent).absolute()
        if not templates_parent_path.is_dir():
            raise ValueError(
                f"Error while loading template: {template_path}:\n"
                f"Jinja template directory not found: "
                f"{templates_parent_path.absolute()}"
            )
        load_jinja_template.environment = environment = Environment(
            loader=FileSystemLoader(templates_parent_path),
            autoescape=select_autoescape(),
            undefined=DebugChainableUndefined,
        )
        return environment.get_template(template_path.name)


def get_format_string_arguments(format_string: str) -> typing.List[str]:
    return [t[1] for t in string.Formatter().parse(format_string) if t[1] is not None]


def strip_margin(text: str) -> str:
    """

    Args:
      text: str:

    Returns:

    """

    return re.sub(r"\n[ \t]*\|", "\n", text.strip().lstrip("|"))


def sha256_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def exponential_backoff(
    retry_iteration: int, max_retry_duration: int = MAXIMUM_EXPONENTIAL_BACKOFF_SECONDS
):
    retry_duration = 2 ** retry_iteration + random.random()
    if retry_duration <= max_retry_duration:
        time.sleep(retry_duration)
    else:
        raise RuntimeError("Maximum exponential backoff duration exceeded.")


def make_archive(source, destination, keep_top_level_folder=True):
    source = str(source)
    destination = str(destination)
    base = os.path.basename(destination)
    name = base.split(".")[0]
    format = base.split(".")[1]
    if keep_top_level_folder:
        archive_from = os.path.dirname(source)
        archive_to = os.path.basename(source.strip(os.sep))
        shutil.make_archive(name, format, archive_from, archive_to)
    else:
        shutil.make_archive(name, format, source)
    shutil.move("{}.{}".format(name, format), destination)


def update_dict(dict1: dict, dict2: dict) -> dict:
    output_dict = {}
    for key in dict1.keys():
        output_dict.update({key: []})
        if type(dict1[key]) != str:
            for value in dict1[key]:
                output_dict[key].append(value)
        else:
            output_dict[key].append(dict1[key])
        if type(dict2[key]) != str:
            for value in dict2[key]:
                output_dict[key].append(value)
        else:
            output_dict[key].append(dict2[key])

    return output_dict
