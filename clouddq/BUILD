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

load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@py_deps//:requirements.bzl", "requirement")
load("@rules_python//python/pip_install:requirements.bzl", "compile_pip_requirements")

filegroup(
    name = "dbt_template",
    srcs = glob(["templates/dbt/models/data_quality_engine/**/*.sql"]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "dbt_project_template",
    srcs = glob(["templates/dbt/dbt_project.yml"]),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "macros_template",
    srcs = glob(["templates/dbt/macros/*.sql"]),
    visibility = ["//visibility:public"],
)

DATA = [
    "//:dbt",
    "//:dbt_project",
    ":dbt_template",
    ":dbt_project_template",
    ":macros_template",
]

py_binary(
    name = "clouddq",
    srcs = ["main.py"],
    data = DATA,
    main = "main.py",
    python_version = "PY3",
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":clouddq_lib",
        ":dbt_runners",
        ":integration",
        requirement("click"),
        requirement("coloredlogs"),
        requirement("sqlite-utils"),
        requirement("markupsafe"),
    ],
)

py_library(
    name = "clouddq_lib",
    srcs = glob(["*.py"]),
    data = [":macros_template"],
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":classes",
        requirement("pyyaml"),
        requirement("jinja2"),
        requirement("dbt-bigquery"),
        requirement("google-cloud-logging"),
        requirement("markupsafe"),
    ],
)

py_library(
    name = "classes",
    srcs = glob(["classes/*.py"]),
    visibility = ["//tests:__subpackages__"],
    deps = [
        requirement("jinja2"),
        requirement("sqlite-utils"),
        requirement("markupsafe"),
    ],
)

py_library(
    name = "dbt_runners",
    srcs = glob(["runners/dbt/*.py"]),
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":clouddq_lib",
        requirement("dbt-bigquery"),
        requirement("pyyaml"),
    ],
)

py_library(
    name = "integration",
    srcs = glob(["integration/**/*.py"]),
    visibility = ["//tests:__subpackages__"],
    deps = [
        requirement("requests"),
        requirement("requests-oauth2"),
        requirement("google-cloud-storage"),
        requirement("google-cloud-bigquery"),
        requirement("google-api-python-client"),
        requirement("pyrate_limiter"),
    ],
)

compile_pip_requirements(
    name = "requirements",
)
