#!/bin/bash
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

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$ROOT/scripts/common.sh"

BASE_NAME="clouddq"
TMP_PATH="/tmp/fix_bazel_zip"

fix_tmp_path() {
  local TEMP_PATH=$1
  echo $TEMP_PATH
  if [[ $(grep -c "PYTHON_BINARY = '/usr/bin/python3'" "${TEMP_PATH}"/__main__.py) == 1 ]]; then
    sed -i "s/PYTHON_BINARY = '\/usr\/bin\/python3'/PYTHON_BINARY = sys.executable/g" "${TEMP_PATH}"/__main__.py
  else
    err "Failed to fix file ${TEMP_PATH}/__main__.py. Expected string to replace not found: PYTHON_BINARY = '/usr/bin/python3'"
    exit 1
  fi
  if [[ $(grep -c 'f"dbt-{dbt_version}"' "${TEMP_PATH}"/runfiles/py_deps/pypi__dbt_bigquery/dbt/adapters/bigquery/connections.py) == 1 ]]; then
    sed -i 's/f"dbt-{dbt_version}"/"Product_Dataplex_CloudDQ\/1.0 (GPN:Dataplex)"/g' "${TEMP_PATH}"/runfiles/py_deps/pypi__dbt_bigquery/dbt/adapters/bigquery/connections.py
  else
    err 'Failed to file ${TEMP_PATH}/runfiles/py_deps/pypi__dbt_bigquery/dbt/adapters/bigquery/connections.py. Expected string to replace not found: f"dbt-{dbt_version}"'
    exit 1
  fi
}

fix_bazel_zip() {
  echo "Usage: fix_bazel_zip archive.zip"
  rm -rf "${TMP_PATH}"/*
  unzip -qq "$1" -d /tmp/fix_bazel_zip
  rm "${TMP_PATH}"/runfiles/"${BASE_NAME}"/__init__.py
  fix_tmp_path "${TMP_PATH}"
  cd "${TMP_PATH}"/ && zip -qq -r "${BASE_NAME}".zip *
  cd -
  cp "${TMP_PATH}"/"${BASE_NAME}".zip "${BASE_NAME}"_patched.zip
}

create_hashsum() {
  sha256sum "$1" | cut -d' ' -f1 > "$1".hashsum
}

if [[ -f "$ROOT"/bazel-bin/"${BASE_NAME}"/"${BASE_NAME}".zip ]]; then
  fix_bazel_zip "$ROOT"/bazel-bin/"${BASE_NAME}"/"${BASE_NAME}".zip
  create_hashsum "${BASE_NAME}"_patched.zip
fi
