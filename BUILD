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
    srcs = glob(["dbt_project.yml"]),
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
    name = "macros",
    srcs = glob(["macros/*.sql"]),
    visibility = ["//visibility:public"],
)
