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

load("@rules_python//python:defs.bzl", "py_test")
load("@py_deps//:requirements.bzl", "requirement")

filegroup(
    name = "resources",
    srcs = glob(["resources/*"]),
    visibility = ["//tests:__subpackages__"],
)

filegroup(
    name = "test_data",
    srcs = glob(["data/**/*"]),
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
    "//clouddq:integration",
    requirement("py"),
    requirement("pytest"),
    requirement("pytest-xdist"),
    requirement("click"),
    requirement("coloredlogs"),
    requirement("google-api-python-client"),
    requirement("requests-oauth2"),
    requirement("filelock"),
    requirement("MarkupSafe"),
]

SRCS = glob([
    "test_*.py",
    "unit/test_*.py",
    "integration/test_*.py",
]) + [
    "compiler_version_test.py",
    "conftest.py",
]

DATA = [
    ":resources",
    "//:dbt",
    "//:docs",
    "//:dbt_project",
    ":test_configs",
    ":test_data",
    ":test_dbt_profiles_dir",
    "//clouddq:dbt_template",
    "//clouddq:dbt_project_template",
    "//clouddq:macros_template",
]

py_test(
    name = "compiler_version_test",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

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
    name = "test_lib",
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
    name = "test_bigquery_client",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dataplex_task",
    srcs = SRCS,
    data = DATA + [
        "//:bazel_bin_clouddq_executable",
    ],
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "conftest",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dataplex_metadata",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dataplex_metadata_uri_templates",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dataplex_entity",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_metadata_uri",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_utils",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_metadata_registry_defaults",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dataplex_dq_configs_cache",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dataplex_integration_performance",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dq_rules",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_advanced_dq_rules",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_dbt_runner",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)

py_test(
    name = "test_reference_columns",
    srcs = SRCS,
    data = DATA,
    legacy_create_init = 0,
    deps = DEPS,
)
