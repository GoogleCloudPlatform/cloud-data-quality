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

function main() {
    set -x
    set -o errexit
    set -o nounset
    ls -la
    find . | sed -e "s/[^-][^\/]*\// |/g" -e "s/|\([^ ]\)/|-\1/"
    cat /etc/*-release
    DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y tzdata sudo curl  > /dev/null
    source scripts/install_development_dependencies.sh --silent > /dev/null
    source scripts/install_python3.sh "3.9.7" > /dev/null
    pyenv virtualenv "3.9.7" clouddq -f
    pyenv shell
    ls -la ~/.pyenv/shims/python3.9
    sudo ln -sf ~/.pyenv/shims/python3.9 /usr/bin/python3
    sudo ln -sf ~m/.pyenv/shims/python3.9 /usr/bin/python
    python3 --version
    python --version
    pip3 --version
    python3 -c 'import sys; print(sys.version_info)'
    python -c 'import sys; print(sys.version_info)'
    echo "common --remote_cache=https://storage.googleapis.com/${_GCS_BAZEL_CACHE}" >> .bazelrc
    echo "common --google_default_credentials" >> .bazelrc
    env
    make addlicense
    make build
    make check
    make test-pip-install
    make test -- --run-dataplex
}

main