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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/scripts/common.sh"

if [[ ! "$OSTYPE" == "linux-gnu"* ]]; then
    echo "This script is only tested to work on Debian/Ubuntu. Developing CloudDQ on OS type '${OSTYPE}' is not currently supported. Exiting..."
    exit 1
fi

function main() {
    local PYTHON_VERSION=3.8
    if ! [[ "${1}" == "3.8" || "${1}" == "3.9" ]]; then
        err "This script is only tested to install python version 3.8 or 3.9. Exiting..."
        exit 1
    else
        local PYTHON_VERSION="${1}"
    fi
    if ! [[ $(uname -v) == *"Debian"* ]]; then
        sudo apt-get install -y software-properties-common > /dev/null
        sudo add-apt-repository -y 'ppa:deadsnakes' > /dev/null || true
        sudo apt-get update > /dev/null
    fi
    sudo apt-get install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-dev \
        python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-distutils > /dev/null
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 2
    sudo update-alternatives --auto python3
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py | python3
}

main "$1"