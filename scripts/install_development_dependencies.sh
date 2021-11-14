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
. "$ROOT/scripts/common.sh"

if [[ ! "$OSTYPE" == "linux-gnu"* ]]; then
    err "This script is only tested to work on Debian/Ubuntu. Developing CloudDQ on OS type '${OSTYPE}' is not currently supported. Exiting..."
    exit 1
fi

SILENT=false
if [[ $# -eq 0 ]]; then
  echo "SILENT: ${SILENT}"
else
  if [[ "$1" == "--silent" ]]; then
      SILENT=true
      echo "SILENT: ${SILENT}"
  fi
fi

# Install Python dependencies
DEBIAN_FRONTEND=noninteractive sudo apt-get update > /dev/null
DEBIAN_FRONTEND=noninteractive sudo apt-get install -yq make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev  \
zip unzip git

# Install golang for building Bazelisk
if ! [ -x "$(command -v "go")" ]; then
    curl -Lo  /tmp/go1.16.5.linux-amd64.tar.gz https://golang.org/dl/go1.16.5.linux-amd64.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go1.16.5.linux-amd64.tar.gz
    export PATH=$PATH:/usr/local/go/bin
    if [[ $SILENT = false ]]; then
      echo "Modify profile to update your \$PATH in ~/.bashrc with golang binary?"
      read -n1 -p "Do you want to continue (Y/n)?" user_input
      if [[ $user_input == "Y" || $user_input == "y" ]]; then
          echo '' >> ~/.bashrc
          echo '# set path to golang binary' >> ~/.bashrc
          echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
          echo '~/.bashrc has been updated.'
          echo '==> Start a new shell or run "source ~/.bashrc" for the changes to take effect.'
      fi
    fi
fi
