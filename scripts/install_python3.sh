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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$ROOT/scripts/common.sh"

if [[ ! "$OSTYPE" == "linux-gnu"* ]]; then
    echo "This script is only tested to work on Debian/Ubuntu. Developing CloudDQ on OS type '${OSTYPE}' is not currently supported. Exiting..."
    exit 1
fi

function main() {
    local PYTHON_VERSION=3.8.12
    if ! [[ "${1}" == "3.8"* || "${1}" == "3.9"* ]]; then
        err "This project is only tested to install python version 3.8. or 3.9. Exiting..."
        exit 1
    else
        local PYTHON_VERSION="${1}"
    fi
    if [ ! -x "$(command -v pyenv)" ]; then
        rm -rf $HOME/.pyenv
        curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
    fi
    export PYENV_ROOT="$HOME/.pyenv"
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init --path)"
    eval "$(pyenv init -)"
    pyenv install "$PYTHON_VERSION"
    pyenv global "$PYTHON_VERSION"
    pyenv shell "$PYTHON_VERSION"
    pyenv version
    python3 --version
}

main "$1"