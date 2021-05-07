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

#set -o errexit
#set -o nounset
#set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/common.sh"

function bazel_test() {
  confirm_gcloud_login || (( err "Failed to retrieve gcloud application-default credentials." && exit 1 ))
  if [[ -z "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
    err "Environment variable GOOGLE_CLOUD_PROJECT not found."
    local GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
    if [[ -z "${GOOGLE_CLOUD_PROJECT}" ]]; then
      err "Unable to find gcloud project config."
      exit 1
    fi
    err "Temporarily setting GOOGLE_CLOUD_PROJECT to: ${GOOGLE_CLOUD_PROJECT}"
  fi
  if [[ -z "${GOOGLE_SDK_CREDENTIALS:-}" ]]; then
    err "Environment variable GOOGLE_SDK_CREDENTIALS not found."
    if [[ -f ~/.config/gcloud/application_default_credentials.json ]]; then
       local GOOGLE_SDK_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
    else
      err "Unable to find gcloud application-default credentials at location '~/.config/gcloud/application_default_credentials.json'."
      exit 1
    fi
    err "Temporarily setting GOOGLE_SDK_CREDENTIALS to: ${GOOGLE_SDK_CREDENTIALS}"
  fi
  set -x
  bin/bazelisk test \
    --test_env GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}" \
    --test_env GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_SDK_CREDENTIALS}" \
    //tests"${1:-/...}"
  set +x
}

if [[ -n "${1:-}" ]]; then
  bazel_test ":$1"
else
  bazel_test
fi
