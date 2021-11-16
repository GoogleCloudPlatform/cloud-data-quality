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

load("@bazel_tools//tools/python:toolchain.bzl", "py_runtime_pair")

py_runtime(
    name = "my_py3_runtime",
    interpreter_path = "/usr/bin/python3",
    python_version = "PY3",
)

py_runtime_pair(
    name = "my_py_runtime_pair",
    py3_runtime = ":my_py3_runtime",
)

toolchain(
    name = "my_toolchain",
    toolchain = ":my_py_runtime_pair",
    toolchain_type = "@bazel_tools//tools/python:toolchain_type",
)

filegroup(
    name = "pyproject_toml",
    srcs = ["pyproject.toml"],
    visibility = ["//tools/lint:__pkg__"],
)

filegroup(
    name = "dbt",
    srcs = glob(["dbt/models/data_quality_engine/**/*.sql"]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "dbt_project",
    srcs = glob(["dbt/dbt_project.yml"]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "configs",
    srcs = glob([
        "configs/**/*.yaml",
        "configs/**/*.yml",
    ]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "bazel_bin_clouddq_executable",
    srcs = ["clouddq_patched.zip"],
    visibility = ["//visibility:public"],
)
