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

load("lint.bzl", "lint")
load("@py_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_binary")

package(default_visibility = ["//visibility:public"])

lint(
    name = "lint",
    mode = "lint",
)

lint(
    name = "check",
    mode = "check",
)

exports_files(["lint.tpl"])

py_binary(
    name = "pyupgrade_py",
    srcs = ["pyupgrade_python.py"],
    main = "pyupgrade_python.py",
    deps = [
        requirement("pyupgrade"),
        requirement("tokenize-rt"),
    ],
)

genrule(
    name = "pyupgrade",
    srcs = [],
    outs = ["pyupgrade"],
    cmd = "echo '$(location :pyupgrade_py) \"$$@\"' > $@",
    executable = True,
    tools = [
        ":pyupgrade_py",
    ],
)

py_binary(
    name = "black_py",
    srcs = ["black_python.py"],
    main = "black_python.py",
    deps = [
        requirement("black"),
        requirement("regex"),
        requirement("appdirs"),
        requirement("click"),
        requirement("toml"),
        requirement("typed_ast"),
        requirement("pathspec"),
    ],
)

genrule(
    name = "black",
    srcs = [],
    outs = ["black"],
    cmd = "echo '$(location :black_py) \"$$@\"' > $@",
    executable = True,
    tools = [
        ":black_py",
    ],
)

py_binary(
    name = "isort_py",
    srcs = ["isort_python.py"],
    data = ["//:pyproject_toml"],
    main = "isort_python.py",
    deps = [
        requirement("isort"),
    ],
)

genrule(
    name = "isort",
    srcs = [],
    outs = ["isort"],
    cmd = "echo '$(location :isort_py) \"$$@\"' > $@",
    executable = True,
    tools = [
        ":isort_py",
    ],
)

py_binary(
    name = "flake8_py",
    srcs = ["flake8_python.py"],
    data = ["//:pyproject_toml"],
    main = "flake8_python.py",
    deps = [
        requirement("flakehell"),
    ],
)

genrule(
    name = "flake8",
    srcs = [],
    outs = ["flake8"],
    cmd = "echo '$(location :flake8_py) \"$$@\"' > $@",
    executable = True,
    tools = [
        ":flake8_py",
    ],
)
