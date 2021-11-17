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

set -x
set -o errexit
set -o nounset

if [[ ! "$OSTYPE" == "linux-gnu"* ]]; then
    echo "This script is only tested to work on Debian/Ubuntu. Developing CloudDQ on OS type '${OSTYPE}' is not currently supported. Exiting..."
    exit 1
fi

GCS_BAZEL_CACHE="${GCS_BAZEL_CACHE}"
PYTHON_VERSION="3.9.7"
PYTHON_VERSION_MINOR="${PYTHON_VERSION%.*}"

function main() {
    if ! [[ "${1}" == "3.8"* || "${1}" == "3.9"* ]]; then
        err "This project is only tested to install python version 3.8. or 3.9. Exiting..."
        exit 1
    else
        local PYTHON_VERSION="${1}"
        local PYTHON_VERSION_MINOR="${PYTHON_VERSION%.*}"
    fi
    env 
    cat /etc/*-release
    sudo rm -f /usr/bin/lsb_release || true
    . ./scripts/install_development_dependencies.sh --silent # > /dev/null
    . ./scripts/install_python3.sh "${PYTHON_VERSION}" # > /dev/null
    pyenv virtualenv "${PYTHON_VERSION}" clouddq -f
    pyenv shell clouddq
    ls -la ~/.pyenv/shims/python"${PYTHON_VERSION_MINOR}"
    sudo ln -sf ~/.pyenv/shims/python"${PYTHON_VERSION_MINOR}" /usr/bin/python3
    sudo ln -sf ~/.pyenv/shims/python"${PYTHON_VERSION_MINOR}" /usr/bin/python
    which /usr/bin/python3
    python3 --version
    python --version
    pip3 --version
    python3 -m pip --version
    python3 -c 'import sys; print(sys.version_info)'
    python -c 'import sys; print(sys.version_info)'
    echo "common --remote_cache=https://storage.googleapis.com/${GCS_BAZEL_CACHE}" >> .bazelrc
    echo "common --google_default_credentials" >> .bazelrc
    make addlicense
    make check
    make test-pip-install
    make build
    ls -la
    make test
}

main "$1"