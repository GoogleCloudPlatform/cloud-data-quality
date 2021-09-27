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

load("@rules_python//python:defs.bzl", "py_test")
load("@py_deps//:requirements.bzl", "requirement")

py_test(
    name = "compiler_version_test",
    srcs = ["compiler_version_test.py"],
    legacy_create_init = 0,
    deps = [
        requirement("pytest"),
    ],
)

filegroup(
    name = "resources",
    srcs = glob(["resources/*"]),
    visibility = ["//tests:__subpackages__"],
)

filegroup(
    name = "test_configs",
    srcs = glob(["resources/configs/**/*"]),
    visibility = ["//tests:__subpackages__"],
)

filegroup(
    name = "test_dbt_profiles_dir",
    srcs = glob(["resources/test_dbt_profiles_dir/profiles.yml"]),
    visibility = ["//tests:__subpackages__"],
)

DEPS = [
    "//clouddq:classes",
    "//clouddq:clouddq_lib",
    "//clouddq:dbt_runners",
    requirement("py"),
    requirement("pytest"),
    requirement("click"),
    requirement("click-logging"),
]

SRCS = glob([
    "unit/test_*.py",
    "integration/test_*.py",
]) + ["conftest.py"]

DATA = [
    ":resources",
    "//:dbt",
    "//:dbt_project",
    ":test_configs",
    ":test_dbt_profiles_dir",
    "//clouddq:dbt_template",
    "//clouddq:dbt_project_template",
    "//clouddq:macros_template",
]

py_test(
    name = "test_classes",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_templates",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_cli_unit",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_cli_integration",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dbt_bigquery",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_bigquery_dry_run_client",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)
