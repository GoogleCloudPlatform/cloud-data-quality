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

# "---------------------------------------------------------"
# "-                                                       -"
# "-  Common commands for all scripts                      -"
# "-                                                       -"
# "---------------------------------------------------------"

# Confirm gcloud is installed and available as a command.
function check_gcloud() {
  if [ ! -x "$(command -v gcloud)" ]; then
    err "gcloud command is not available. Please install gcloud before continuing"
    err "Please visit: https://cloud.google.com/sdk/install"
    exit 1
  fi
}

# Confirm golang is installed and available as a command.
function check_go() {
  if [ ! -x "$(command -v go version)" ]; then
    err "golang is not available. It is used to install bazelisk and bazel for your platform."
    err "Please install golang before continuing: https://golang.org/doc/install"
    exit 1
  fi
}

# Confirm python is installed and available as a command.
# Bazel builds the python binary including the interpreter from scratch
# so it doesn't matter which version of Python 3 is installed.
function check_python3() {
  if [ ! -x "$(command -v python3 --version)" ]; then
    err "python3 command is not available. It is used to run the Python zip binary."
    err "Please install python3 before continuing: https://docs.python.org/3/installing/index.html"
    exit 1
  fi
}

# Print error message to stderr
function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

# Check to see that a required environment variable is set.  
# Use it without the $, as in:
#   require_env_var VARIABLE_NAME  
# or   
#   require_env_var VARIABLE_NAME "Some description of the variable"
function require_env_var {
  var_name="${1:-}"
  if [ -z "${!var_name:-}" ]; then
    echo "[X] The required env variable ${var_name} is empty"
    if [ ! -z "${2:-}" ]; then        
       echo "  - $2"     
    fi
    exit 1
  fi
}

# Check to see that we have a required binary on the path
function require_binary {
  if [ -z "${1:-}" ]; then
    echo "${FUNCNAME[0]} requires an argument"
    exit 1
  fi
  if ! [ -x "$(command -v "$1")" ]; then
    echo "The required executable '$1' is not on the path."
    exit 1
  fi
}

###########################################################
#                    gcloud utilities                     #
###########################################################
# Log into gcloud
function gcloud_login() {
  gcloud auth application-default login --no-launch-browser
  if [[ $? -ne 0 ]]; then
    err "gcloud login failed. Aborting"
    exit 1
  fi
}

# Handle no application-default credentials scenario
function handle_no_token_error() {
  err "ERROR: No active gcloud application-default credentials found."
  err "To continue testing, please ensure a valid authentication credential exists to access GCP from client libraries."
  err
  err "You can set the GOOGLE_APPLICATION_CREDENTIALS environment variable to a service-account json key to continue"
  err "Alternatively, you may authenticate using your own user credentials using 'gcloud auth application-default login'."
  err "NOTE: It is not recommended to use either of the above methods of authentication in Production."
  err "See details here https://cloud.google.com/docs/authentication/production."
  err
  read -p "Run 'gcloud auth application-default login' to use your user account as application-default? (Y/y)" -n 1 -r
  err
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    gcloud_login
  else
    err "Exiting..."
    exit 2
  fi
}

# Get account used for application client library
function get_application_client_account() {
  check_gcloud
  check_python3
  if [[ -f "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
    err "Authenticating to GCP using GOOGLE_APPLICATION_CREDENTIALS at path $GOOGLE_APPLICATION_CREDENTIALS."
    export GOOGLE_SDK_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS"
    if [[ $(cat "$GOOGLE_APPLICATION_CREDENTIALS") == *"client_email"* ]]; then
      local GCLOUD_SDK_ACCOUNT=$(python3 -c "import sys, json; print(json.load(sys.stdin)['client_email'])" <"$GOOGLE_APPLICATION_CREDENTIALS")
      err "Environment variable GOOGLE_APPLICATION_CREDENTIALS currently set to a json credential for account: $GCLOUD_SDK_ACCOUNT"
    else
      err "Invalid GOOGLE_APPLICATION_CREDENTIALS. Exiting..."
      exit 1
    fi
  elif [[ -f ~/.config/gcloud/application_default_credentials.json ]]; then
    err "Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS at path $GOOGLE_APPLICATION_CREDENTIALS."
    local access_token=$(gcloud auth application-default print-access-token)
    if [ -z "${!access_token:-}" ]; then
      err "Failed to get authentication token with 'gcloud auth application-default print-access-token'."
      exit 1
    fi
    local access_details=$(curl -s https://www.googleapis.com/oauth2/v3/tokeninfo?access_token="$access_token")
    if [[ "$access_details" == *"email"* ]]; then
      local GCLOUD_SDK_ACCOUNT=$(echo "$access_details" | python3 -c "import sys, json; print(json.load(sys.stdin)['email'])")
      err "User account in use for 'gcloud auth application-default print-access-token': ${GCLOUD_SDK_ACCOUNT}"
      export GOOGLE_SDK_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
      echo
      err "Setting environment variable GOOGLE_SDK_CREDENTIALS to '~/.config/gcloud/application_default_credentials.json'."
    else
      err "Failed to retrieve active application_default credentials for the BigQuery client SDK."
      exit 1
    fi
  fi
}

# Confirm the user is already logged into gcloud, if they aren't
# attempt to login.
function confirm_gcloud_login() {
  err "Checking gcloud login state..."
  err
  GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
  export GOOGLE_CLOUD_PROJECT
  err "Using GCP project: $GOOGLE_CLOUD_PROJECT"
  gcloud auth application-default print-access-token 1>/dev/null || handle_no_token_error
  get_application_client_account
  echo
}
