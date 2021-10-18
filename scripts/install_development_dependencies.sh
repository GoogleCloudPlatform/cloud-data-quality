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

if [[ ! "$OSTYPE" == "linux-gnu"* ]]; then
    err "This script is only tested to work on Debian/Ubuntu. Developing CloudDQ on OS type '${OSTYPE}' is not currently supported. Exiting..."
    exit 1
fi

# Install sandboxfs
apt-get update
apt install -y libfuse2
apt install -y curl
curl -Lo /tmp/sandboxfs-0.2.0.tgz https://github.com/bazelbuild/sandboxfs/releases/download/sandboxfs-0.2.0/sandboxfs-0.2.0-20200420-linux-x86_64.tgz
tar xzv -C /usr/local -f /tmp/sandboxfs-0.2.0.tgz
rm /tmp/sandboxfs-0.2.0.tgz
sandboxfs --help

ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata

# Install Python dependencies
apt-get update;  apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# Install golang for building Bazelisk
curl -Lo  /tmp/go1.16.5.linux-amd64.tar.gz https://golang.org/dl/go1.16.5.linux-amd64.tar.gz
rm -rf /usr/local/go
tar -C /usr/local -xzf /tmp/go1.16.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go version