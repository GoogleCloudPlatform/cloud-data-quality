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

DEPS = [
    "//clouddq:classes",
    "//clouddq:clouddq_lib",
    requirement("py"),
    requirement("pytest"),
    requirement("click"),
]

SRCS = glob([
    "test_*.py",
]) + ["conftest.py"]

DATA = [
    ":resources",
    "//:configs",
    "//:dbt",
    "//:macros",
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

filegroup(
    name = "resources",
    srcs = glob(["resources/*"]),
    visibility = ["//visibility:public"],
)

py_test(
    name = "test_cli",
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
