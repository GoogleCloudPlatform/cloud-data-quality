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

# "---------------------------------------------------------"
# "-                                                       -"
# "-  Common commands for all scripts                      -"
# "-                                                       -"
# "---------------------------------------------------------"

# Confirm gcloud is installed and available as a command.
if [ ! -x "$(command -v gcloud)" ]; then
  echo "gcloud command is not available. Please install gcloud before continuing"
  echo "Please visit: https://cloud.google.com/sdk/install"
  exit 1
fi

# Confirm golang is installed and available as a command.
if [ ! -x "$(command -v go version)" ]; then
  echo "golang is not available. It is used to install bazelisk and bazel for your platform."
  echo "Please install golang before continuing: https://golang.org/doc/install"
  exit 1
fi

# Confirm python is installed and available as a command.
# Bazel builds the python binary including the interpreter from scratch
# so it doesn't matter which version of Python 3 is installed.
if [ ! -x "$(command -v python3 --version)" ]; then
  echo "python3 command is not available. It is used to run the Python zip binary."
  echo "Please install python3 before continuing: https://docs.python.org/3/installing/index.html"
  exit 1
fi

###########################################################
#                    gcloud utilities                     #
###########################################################
# Log into gcloud
function gcloud_login() {
  gcloud auth application-default login --no-launch-browser
  if [[ $? -ne 0 ]]; then
    echo "gcloud login failed. Aborting"
  fi
}

# Handle no application-default credentials scenario
function handle_no_token_error() {
  echo "ERROR: gcloud application credential not currently active."
  echo "Please ensure a valid authentication credential exists to access GCP from client libraries."
  echo
  echo "You can set the GOOGLE_APPLICATION_CREDENTIALS environment variable to a service-account json key if developing on environments other than GCE, GKE, App Engine, Cloud Run, and Cloud Functions."
  echo "Alternatively, you may authenticate using your own user credentials using 'gcloud auth application-default login'. This is recommended for testing using your own credentials only, and not for production use."
  echo "See details here https://cloud.google.com/docs/authentication/production."
  echo
  read -p "Run 'gcloud auth application-default login' to use your user account as application-default? (Y/y)" -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    gcloud_login
  else
    echo "Exiting..."
    exit 2
  fi
}

# Get account used for application client library
function get_application_client_account() {
  if [[ -f "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
    echo "Authenticating to GCP using GOOGLE_APPLICATION_CREDENTIALS at path $GOOGLE_APPLICATION_CREDENTIALS."
    export GOOGLE_SDK_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS"
    if [[ $(cat "$GOOGLE_APPLICATION_CREDENTIALS") == *"client_email"* ]]; then
      GCLOUD_SDK_ACCOUNT=$(python3 -c "import sys, json; print(json.load(sys.stdin)['client_email'])" <"$GOOGLE_APPLICATION_CREDENTIALS")
      export GCLOUD_SDK_ACCOUNT
      echo "Environment variable GOOGLE_APPLICATION_CREDENTIALS currently set to a json credential for account: $GCLOUD_SDK_ACCOUNT"
    else
      echo "Invalid GOOGLE_APPLICATION_CREDENTIALS. Exiting..."
      exit 2
    fi
  elif [[ -f ~/.config/gcloud/application_default_credentials.json ]]; then
    access_token=$(gcloud auth application-default print-access-token) || echo "ERROR: cannot execute 'gcloud auth application-default print-access-token'."
    access_details=$(curl -s https://www.googleapis.com/oauth2/v3/tokeninfo?access_token="$access_token")
    if [[ "$access_details" == *"email"* ]]; then
      GCLOUD_SDK_ACCOUNT=$(echo "$access_details" | python3 -c "import sys, json; print(json.load(sys.stdin)['email'])")
      export GCLOUD_SDK_ACCOUNT
      echo "User account in use for 'gcloud auth application-default print-access-token': $GCLOUD_SDK_ACCOUNT"
      export GOOGLE_SDK_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
    else
      echo "ERROR: failed to retrieve active application_default credentials. Exiting..."
      exit 2
    fi
  fi
}

# Confirm the user is already logged into gcloud, if they aren't
# attempt to login.
function confirm_gcloud_login() {
  echo "Checking gcloud login state..."
  echo
  GOOGLE_CLOUD_PROJECT=$(gcloud config list --format="value(core.project)")
  export GOOGLE_CLOUD_PROJECT
  echo "Using GCP project: $GOOGLE_CLOUD_PROJECT"
  gcloud auth application-default print-access-token 1>/dev/null || handle_no_token_error
  get_application_client_account
  echo
}
