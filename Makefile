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
	bin/addlicense -check clouddq tests tools macros scripts dbt

.PHONY: clean
clean: bin/bazelisk ## clean build artifacts
	bin/bazelisk clean --async --expunge

.PHONY: buildzip
buildzip: bin/bazelisk  ## build zip executable and apply patch to fix init issue (NOTE: this fails if sandboxfs cannot be found in $PATH)
	bin/bazelisk build //clouddq:clouddq --output_groups=python_zip_file --experimental_use_sandboxfs --sandbox_debug --sandbox_fake_username --sandbox_fake_hostname
	@source scripts/fix_bazel_zip.sh

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

.PHONY: check
check: bin/bazelisk ## check code with black, buildifier, pyupgrade, and flake8
	bin/bazelisk run //tools/lint:check $(RUN_ARGS)

.PHONY: lint
lint: bin/bazelisk ## apply black, buildifier, pyupgrade, and flake8
	bin/bazelisk run //tools/lint:lint $(RUN_ARGS)

.PHONY: build
build: ## build zip executable and run it without arguments to show the help text
	$(MAKE) buildzip
	python bazel-bin/clouddq/clouddq_patched.zip --help

.PHONY: help
help: ## show help text
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

.DEFAULT_GOAL := help
