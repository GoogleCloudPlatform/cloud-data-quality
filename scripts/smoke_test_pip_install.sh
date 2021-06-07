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

set -x

# get diagnostic info
which python3
python3 --version
python3 -m pip --version

# make sure wheel is installed
pip install wheel --upgrade --force
pip install setuptools --upgrade --force
python3 -m pip install wheel --upgrade --force
python3 -m pip install setuptools --upgrade --force
python3 -c "import setuptools; print(setuptools.__version__)"

# install poetry
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

# create temporary virtualenv
python3 -m venv /tmp/clouddq_test_env
source /tmp/clouddq_test_env/bin/activate

# install clouddq wheel into temporary env
python3 -m pip install .

# smoke test clouddq commands
python3 clouddq --help
python3 clouddq ALL configs --dry_run