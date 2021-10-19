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

#check_go
# Install golang for building Bazelisk
curl -Lo  /tmp/go1.16.5.linux-amd64.tar.gz https://golang.org/dl/go1.16.5.linux-amd64.tar.gz
rm -rf /usr/local/go
tar -C /usr/local -xzf /tmp/go1.16.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go version

GO111MODULE=off go get github.com/bazelbuild/bazelisk
GO111MODULE=off go build -o bin/bazelisk github.com/bazelbuild/bazelisk
