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

PYTHON_VERSION="3.9.7"
LOCAL_VERSION="$(python --version 2>&1)"
echo "Python version in current shell: $LOCAL_VERSION"

# Confirm poetry is installed and available as a command.
if [ ! -x "$(command -v poetry)" ]; then
  echo "poetry is not available. Please install poetry before continuing"
  echo "Please see: https://python-poetry.org/docs"
  exit 1
fi

# Confirm pyenv is installed and available as a command.
if [ ! -x "$(command -v pyenv)" ]; then
  echo "pyenv is not available. Please install pyenv before continuing"
  echo "Please see: https://github.com/pyenv/pyenv"
  exit 1
fi

# Check python version if 3.8.*, otherwise install it with pyenv.
if [[ "${LOCAL_VERSION%.*}" != "Python ${PYTHON_VERSION%.*}" ]]; then
  echo "Current Python interpreter version $LOCAL_VERSION is not ${PYTHON_VERSION%.*}.*"
  echo
  if ! pyenv versions | grep "$PYTHON_VERSION"; then
    read -p "Install new interpreter version $PYTHON_VERSION with 'pyenv'? (Y/y)" -n 1 -r
    echo
    if [[ "$REPLY" =~ ^[Yy]$ ]]; then
      pyenv install "$PYTHON_VERSION"
    else
      echo "Exiting..."
      exit 2
    fi
  fi
  read -p "Activate python virtualenv with interpreter version $PYTHON_VERSION using 'pyenv'? (Y/y)" -n 1 -r
  echo
  if [[ "$REPLY" =~ ^[Yy]$ ]]; then
    pyenv local "$PYTHON_VERSION"
  else
    echo "Exiting..."
    exit 2
  fi
fi

if pyenv local "$PYTHON_VERSION"; then
  PYPATH=$(pyenv which python)
  export PYPATH
else
  echo "Using system Python Interpreter version $LOCAL_VERSION"
fi

poetry env use "${PY_PATH:-$PYTHON_VERSION}"
echo "Poetry Python Env Info:"
poetry env info
poetry lock
poetry install
poetry shell
