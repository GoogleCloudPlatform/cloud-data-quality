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
# shellcheck source=/dev/null
source "$ROOT/scripts/common.sh"

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_python_deps() {
    apt-get install -y --no-install-recommends make \
     build-essentiallibssl-dev zlib1g-dev \
     libbz2-dev libreadline-dev libsqlite3-dev \
     wget curl llvm libncurses5-dev xz-utils \
     tk-dev libxml2-dev libxmlsec1-dev libffi-dev \
     liblzma-dev
}

function main() {
  # In an HA cluster, only run on -m-0
  if [[ "${HOSTNAME}" == "${DATAPROC_MASTER}" ]]; then
    update_apt_get || err 'Failed to update apt-get'
    install_python_deps || err 'Failed to install python deps'
    echo "Successfully installed python dependencies."
  fi
}

main