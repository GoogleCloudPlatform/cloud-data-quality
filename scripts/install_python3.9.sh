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

if [[ ! "$OSTYPE" == "linux-gnu"* ]]; then
    err "This script is only tested to work on Debian/Ubuntu. Developing CloudDQ on OS type '${OSTYPE}' is not currently supported. Exiting..."
    exit 1
fi

set -x
if ! [ -x "$(command -v "/usr/bin/python3.9")" ]; then
    sudo apt-get install -y software-properties-common ca-certificates > /dev/null
    sudo add-apt-repository -y 'ppa:deadsnakes' > /dev/null || true
    sudo apt-get update > /dev/null
    sudo apt-get install -y python3.9 python3.9-dev python3.9-venv > /dev/null
    sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 2
    sudo update-alternatives --auto python3
fi

sudo ln -s /usr/bin/python3 /usr/bin/python
python3 --version
python --version
python3 -m ensurepip --default-pip
set +x