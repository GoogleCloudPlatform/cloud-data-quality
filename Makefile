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

# Make will use bash instead of sh
SHELL := /usr/bin/env bash

bin/bazelisk: ## install bazel
	@source scripts/build_bazel.sh

bin/addlicense: ## install addlicense
	@source scripts/build_addlicense.sh

install: ## create python virtualen and install clouddq
	@source scripts/poetry_install.sh

.PHONY: addlicense
addlicense: bin/addlicense ## run addlicense check
	bin/addlicense -check clouddq tests tools scripts

.PHONY: clean
clean: bin/bazelisk ## clean build artifacts
	bin/bazelisk clean --async --expunge

# If the first argument is "run", "test, "check", or "lint"...
ifeq ($(firstword $(MAKECMDGOALS)),$(filter $(firstword $(MAKECMDGOALS)),run test check lint))
  # use the rest as arguments
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

.PHONY: test
test: bin/bazelisk  ## run tests, use 'make test <test_name>' to run a single test
	@source scripts/bazel_test.sh $(RUN_ARGS)

.PHONY: test-pip-install
test-pip-install:  ## run tests on pip-install-path
	@source scripts/test_pip_install.sh

.PHONY: test-all
test-all: test-pip-install test  ## run all tests

.PHONY: check
check: bin/bazelisk ## check code with black, buildifier, pyupgrade, and flake8
	bin/bazelisk run //tools/lint:check $(RUN_ARGS)

.PHONY: lint
lint: bin/bazelisk ## apply black, buildifier, pyupgrade, and flake8
	bin/bazelisk run //tools/lint:lint $(RUN_ARGS)

.PHONY: build
build: bin/bazelisk ## build zip executable and run it without arguments to show the help text
	bin/bazelisk build //clouddq:clouddq --output_groups=python_zip_file --sandbox_fake_username --sandbox_fake_hostname
	@source scripts/fix_bazel_zip.sh
	python3 clouddq_patched.zip --help

.PHONY: help
help: ## show help text
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

.DEFAULT_GOAL := help
