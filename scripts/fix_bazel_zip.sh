#!/bin/bash
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

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_NAME="clouddq"

fix_bazel_zip() {
  echo "Usage: fix_bazel_zip archive.zip"
  rm -rf /tmp/fix_bazel_zip/*
  unzip -qq "$1" -d /tmp/fix_bazel_zip
  rm /tmp/fix_bazel_zip/runfiles/"${BASE_NAME}"/__init__.py
  cd /tmp/fix_bazel_zip/ && zip -qq -r "${BASE_NAME}".zip *
  cd -
<<<<<<< HEAD
  cp /tmp/fix_bazel_zip/"${BASE_NAME}".zip "$ROOT"/"${BASE_NAME}"_patched.zip
=======
  cp /tmp/fix_bazel_zip/"${BASE_NAME}".zip "${BASE_NAME}"_patched.zip
>>>>>>> debian-11
}

if [[ -f "$ROOT"/bazel-bin/"${BASE_NAME}"/"${BASE_NAME}".zip ]]; then
  fix_bazel_zip "$ROOT"/bazel-bin/"${BASE_NAME}"/"${BASE_NAME}".zip
fi
