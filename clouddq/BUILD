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

load("@rules_python//python:defs.bzl", "py_binary", "py_library")
load("@py_deps//:requirements.bzl", "requirement")
load("@rules_python//python/pip_install:requirements.bzl", "compile_pip_requirements")

DEPS = [
    ":classes",
    requirement("pyyaml"),
    requirement("jinja2"),
    requirement("dbt-bigquery"),
]

py_binary(
    name = "clouddq",
    srcs = ["main.py"],
    main = "main.py",
    python_version = "PY3",
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":clouddq_lib",
        requirement("click"),
    ],
)

py_library(
    name = "clouddq_lib",
    srcs = glob(["*.py"]),
    data = ["//:macros"],
    visibility = ["//tests:__subpackages__"],
    deps = DEPS,
)

py_library(
    name = "classes",
    srcs = glob(["classes/*.py"]),
    visibility = ["//tests:__subpackages__"],
    deps = [
        requirement("jinja2"),
    ],
)

compile_pip_requirements(
    name = "requirements",
)
