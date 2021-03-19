#! /usr/bin/env bash
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

MODE=@@MODE@@

echo "$MODE: " $@


RUN_BAZEL=no
RUN_BLACK=no
RUN_PYUPGRADE=no
RUN_ISORT=no
RUN_FLAKE8=no
RUN_ENTRIES=all
if [[ $# -eq 0 ]]; then
  RUN_BAZEL=true
  RUN_BLACK=true
  RUN_PYUPGRADE=true
  RUN_ISORT=true
  RUN_FLAKE8=true
else
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "bazel" ]]; then
      shift
      echo "$MODE: " bazel
      RUN_BAZEL=true
    elif [[ "$1" == "black" ]]; then
      shift
      echo "$MODE: " black
      RUN_BLACK=true
    elif [[ "$1" == "pyupgrade" ]]; then
      shift
      echo "$MODE: " pyupgrade
      RUN_PYUPGRADE=true
    elif [[ "$1" == "isort" ]]; then
      shift
      echo "$MODE: " isort
      RUN_ISORT=true
    elif [[ "$1" == "flake8" ]]; then
      shift
      echo "$MODE: " flake8
      RUN_FLAKE8=true
    elif [[ "$1" == "--" ]]; then
      shift
      echo "$MODE: " "--"
      RUN_ENTRIES="--"
      break
    else
      echo "unknown command: $i"
      exit 1
    fi
  done
fi

echo "Selected: Bazel=$RUN_BAZEL Black=$RUN_BLACK Pyupgrade=$RUN_PYUPGRADE isort=$RUN_ISORT flake8=$FLAKE8_PATH [Entries]: $RUN_ENTRIES"

BLACK_PATH=@@BLACK_PATH@@
BUILDIFIER_PATH=@@BUILDIFIER_PATH@@
PYUPGRADE_PATH=@@PYUPGRADE_PATH@@
ISORT_PATH=@@ISORT_PATH@@
FLAKE8_PATH=@@FLAKE8_PATH@@


mode="$MODE"

black_path=$(readlink "$BLACK_PATH")
buildifier_path=$(readlink "$BUILDIFIER_PATH")
pyupgrade_path=$(readlink "$PYUPGRADE_PATH")
isort_path=$(readlink "$ISORT_PATH")
flake8_path=$(readlink "$FLAKE8_PATH")

echo "mode:" $mode
echo "black:" $black_path
echo "buildifier:" $buildifier_path
echo "pyupgrade:" $pyupgrade_path
echo "isort:" $isort_path
echo "flake8:" $flake8_path

black_func() {
  echo $1 $2
  if [[ "$1" == "lint" ]]; then
    $black_path $2
  else
    $black_path --check $2
  fi
}

buildifier_func() {
  echo $1 $2
  if [[ "$1" == "lint" ]]; then
    $buildifier_path -mode=fix $2
  else
    $buildifier_path -mode=check $2
  fi
}

pyupgrade_func() {
  echo $1 $2
  if [[ "$1" == "lint" ]]; then
    $pyupgrade_path --exit-zero-even-if-changed --py3-only $2
  else
    $pyupgrade_path --py3-only $2
  fi
}

isort_func() {
  echo $1 $2
  if [[ "$1" == "lint" ]]; then
    $isort_path --profile black --settings-file pyproject.toml $2
  else
    $isort_path --profile black --settings-file pyproject.toml --check-only $2
  fi
}

flake8_func() {
  echo $1 $2
  if [[ "$1" == "lint" ]]; then
    $flake8_path lint --config=pyproject.toml $2
  else
    $flake8_path lint --config=pyproject.toml $2
  fi
}

set -e

if [[ "$RUN_BLACK" == "true" ]]; then
echo "Run Black"

if [[ "$RUN_ENTRIES" == "--" ]]; then
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
      black_func $mode clouddq tests tools ; \
)
else
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
      black_func $mode clouddq tests tools ; \
)
fi

fi


if [[ "$RUN_BAZEL" == "true" ]]; then
echo "Run Bazel Buildifier"

if [[ "$RUN_ENTRIES" == "--" ]]; then
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
    for i in $@ ; do \
        buildifier_func $mode "$i" ; \
    done \
)
else
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
    for i in \
        $( \
            find . -type f \
                \( \
                    -name '*.bzl' \
                    -o -name '*.sky' \
                    -o -name '*.BUILD' \
                    -o -name 'BUILD.*.bazel' \
                    -o -name 'BUILD.*.oss' \
                    -o -name 'WORKSPACE.*.bazel' \
                    -o -name 'WORKSPACE.*.oss' \
                    -o -name BUILD.bazel \
                    -o -name BUILD \
                    -o -name WORKSPACE \
                    -o -name WORKSPACE.bazel \
                    -o -name WORKSPACE.oss \
                \) \
        ) ; do \
        buildifier_func $mode "$i" ; \
    done \
)
fi

fi

if [[ "$RUN_PYUPGRADE" == "true" ]]; then
echo "Run Pyupgrade"

if [[ "$RUN_ENTRIES" == "--" ]]; then
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
    for i in \
        $( \
            find clouddq tests tools -type f \
                    -name '*.py'
        ) ; do \
        pyupgrade_func $mode "$i" ; \
    done \
)
else
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
    for i in \
        $( \
            find clouddq tests tools -type f \
                    -name '*.py'
        ) ; do \
        pyupgrade_func $mode "$i" ; \
    done \
)
fi

fi

if [[ "$RUN_ISORT" == "true" ]]; then
echo "Run isort"

if [[ "$RUN_ENTRIES" == "--" ]]; then
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
      isort_func $mode clouddq tests tools ; \
)
else
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
      isort_func $mode clouddq tests tools ; \
)
fi

fi


if [[ "$RUN_FLAKE8" == "true" ]]; then
echo "Run flake8"

if [[ "$RUN_ENTRIES" == "--" ]]; then
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
        flake8_func clouddq tests tools ; \
)
else
( \
    cd "$BUILD_WORKSPACE_DIRECTORY" && \
        flake8_func $mode clouddq tests tools ; \
)
fi

fi

exit 0
