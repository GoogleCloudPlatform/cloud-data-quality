# User Manual

## Usage Guide from README

### Overview

CloudDQ is a Command-Line Interface (CLI) application. It takes as input YAML Data Quality configurations, generates and executes SQL code in BigQuery using provided connection configurations, and writes the resulting Data Quality summary statistics to a BigQuery table of your choice.

#### System Requirements

CloudDQ is currently only tested to run on `Ubuntu`/`Debian` linux distributions. It may not work properly on other OS such as `MacOS`, `Windows`/`cygwin`, or `CentOS`/`Fedora`/`FreeBSD`, etc...

For development or trying out CloudDQ, we recommend using either [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell-editor) or a [Google Cloud Compute Engine VM](https://cloud.google.com/compute) with the [Debian 11 OS distribution](https://cloud.google.com/compute/docs/images/os-details#debian).

CloudDQ requires the command `python3` to point to a Python Interterpreter version 3.8.x or 3.9.x. To install the correct Python version, please refer to the script `scripts/poetry_install.sh` for an interactive installation or `scripts/install_python3.sh` for a non-interactive installation intended for automated build/test processes. 

For example, on [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell-editor), you could install Python 3.9 by running:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.5.0"
git clone -b "v${CLOUDDQ_RELEASE_VERSION}" https://github.com/GoogleCloudPlatform/cloud-data-quality.git
source cloud-data-quality/scripts/install_python3.sh "3.9.7"
python3 --version
```

#### Using Pre-Built Executable

The simplest way to run CloudDQ is to use one of the pre-built executable provided in the Github releases page: https://github.com/GoogleCloudPlatform/cloud-data-quality/releases

We currently provide pre-built executables for Debian 11+Python3.9 built for execution on Dataplex Task/Dataproc Serverless Batches (this executable will also work on Debian 10 in Cloud Shell) and Ubuntu18+Python3.8 built for execution as Dataproc Jobs/Workflows with a compatible Ubuntu18 OS image.

For example, from [Cloud Shell](https://shell.cloud.google.com/?show=ide%2Cterminal), you can download the executable with the following commands:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.5.0"
export TARGET_OS="debian_11"  # can be either "debian_11" or "ubuntu_18"
export TARGET_PYTHON_INTERPRETER="3.9"  # can be either "3.8" or "3.9"
cd cloud-data-quality
wget -O clouddq_executable.zip https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v"${CLOUDDQ_RELEASE_VERSION}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}"_"${TARGET_OS}"_python"${TARGET_PYTHON_INTERPRETER}".zip
```

If you do not have Python 3.9 installed, you could install Python 3.9 on an `Ubuntu` or `Debian` machine by running:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.5.0"
git clone -b "v${CLOUDDQ_RELEASE_VERSION}" https://github.com/GoogleCloudPlatform/cloud-data-quality.git
source cloud-data-quality/scripts/install_python3.sh "3.9.7"
python3 --version
```

You can then use the CLI by passing the zip into a Python interpreter:

```bash
#!/bin/bash
python3 clouddq_executable.zip --help
```

This should show you the help text.

#### Example CLI commands

In the below examples, we will use the example YAML `configs` provided in this project.

The example commands will authenticate to GCP with your user's credentials via [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/best-practices-applications#overview_of_application_default_credentials). Ensure you have a [GCP Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) and your user's GCP credentials have at minimum project-level IAM permission to run BigQuery jobs (`roles/bigquery.jobUser`) and to create new BigQuery datasets (`roles/bigquery.dataEditor`) in that project.

From either [Cloud Shell](https://shell.cloud.google.com) or a `Ubuntu`/`Debian` machine, clone the CloudDQ project to get the sample config directory:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.5.0"
git clone -b "v${CLOUDDQ_RELEASE_VERSION}" https://github.com/GoogleCloudPlatform/cloud-data-quality.git
```

Then change directory to the git project and get the pre-built executable from Github:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.5.0"
export TARGET_OS="debian_11"  # can be either "debian_11" or "ubuntu_18"
export TARGET_PYTHON_INTERPRETER="3.9"  # can be either "3.8" or "3.9"
cd cloud-data-quality
wget -O clouddq_executable.zip https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v"${CLOUDDQ_RELEASE_VERSION}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}"_"${TARGET_OS}"_python"${TARGET_PYTHON_INTERPRETER}".zip
```

If you do not have Python 3.9 installed, you could install Python 3.9 on an `Ubuntu` or `Debian` machine by running:

```bash
#!/bin/bash
source cloud-data-quality/scripts/install_python3.sh "3.9.7"
python3 --version
```

Run the following command to authenticate to GCP using application-default credentials. The command will prompt you to login and provide a verification code back into the console.
```bash
#!/bin/bash
gcloud auth application-default login
```

Then locate your [GCP project ID](https://support.google.com/googleapi/answer/7014113) and set it to a local variable for usage later. We will also configure `gcloud` to use this GCP Project ID.

```bash
#!/bin/bash
export GOOGLE_CLOUD_PROJECT="<your_GCP_project_id>"
gcloud config set project ${GOOGLE_CLOUD_PROJECT}
```

Now we'll create a new dataset with a custom name in a location of our choice and load some sample data into it:

```bash
#!/bin/bash
# Create a BigQuery Dataset in a region of your choice and load data
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
# Create BigQuery Dataset. Skip this step if `CLOUDDQ_BIGQUERY_DATASET` already exists
bq --location=${CLOUDDQ_BIGQUERY_REGION} mk --dataset ${GOOGLE_CLOUD_PROJECT}:${CLOUDDQ_BIGQUERY_DATASET}
# Fetch the example csv file
curl -LO https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-data-quality/main/tests/data/contact_details.csv
# Load sample data to the dataset
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details contact_details.csv
```

Before running CloudDQ, we need to edit the table configurations in `configs/entities/test-data.yml` to use our Project ID and BigQuery dataset ID.

```bash
#!/bin/bash
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
sed -i s/\<your_gcp_project_id\>/${GOOGLE_CLOUD_PROJECT}/g configs/entities/test-data.yml
sed -i s/\<your_bigquery_dataset_id\>/${CLOUDDQ_BIGQUERY_DATASET}/g configs/entities/test-data.yml
```

Using the same Project ID, GCP Region, and BigQuery dataset ID as defined before, we will attempt to execute two `Rule Binding`s with the unique identifers `T2_DQ_1_EMAIL` and `T3_DQ_1_EMAIL_DUPLICATE` from the `configs` directory containing the complete YAML configurations:

```bash
#!/bin/bash
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
export CLOUDDQ_TARGET_BIGQUERY_TABLE="<project-id>.<dataset-id>.<table-id>"
python3 clouddq_executable.zip \
    T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE \
    configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
    --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}"
```

By running this CLI command, `CloudDQ` will:
1. validate that the YAML configs are valid, i.e. every `Rule`, `Entity`, and `Row Filter` referenced in each `Rule Binding` exists and there are no duplicated configuration ID. CloudDQ will attempt to parse all files with extensions `yml` and `yaml` discovered in a path and use the top-level keys (e.g. `rules`, `rule_bindings`, `row_filters`, `entities`) to determine the config type. Each configuration item must have a globally unique identifier in the `config` path.
2. validate that the dataset `--gcp_bq_dataset_id` and the table `--target_bigquery_summary_table` (if exists) is located in the region `--gcp_region_id`
3. resolves the `entity_uris` defined in all `Rule Binding` to retrieve the corresponding table and column names for validation
4. generate BigQuery SQL code for each `Rule Binding` and validate that it is valid BigQuery SQL using BigQuery dry-run feature
5. create a BigQuery view for each generated BigQuery SQL `Rule Binding` in the BigQuery dataset specified in `--gcp_bq_dataset_id`
6. create a BigQuery job to execute all `Rule Binding` SQL views. The BigQuery jobs will be created in the GCP project specified in `--gcp_project_id` and GCP region specified in `--gcp_region_id`
7. aggregate the validation outcomes and write the Data Quality summary results into a table called `dq_summary` in the BigQuery dataset specified in `--gcp_bq_dataset_id`
8. if the CLI argument `--target_bigquery_summary_table` is defined, CloudDQ will append all rows created in the `dq_summary` table in the last run to the BigQuery table specified in  `--target_bigquery_summary_table`. `--target_bigquery_summary_table` currently an optional argument to maintain backwards compatibility but will become a required argument in version 1.0.0.
9. if the CLI flag `--summary_to_stdout` is set, CloudDQ will retrieve all validation results rows created in the `dq_summary` table in the last run and log them as a JSON record to Cloud Logging and stdout

To execute all `Rule Binding`s discovered in the config directory, use `ALL` as the `RULE_BINDING_IDS`:

```bash
#!/bin/bash
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
export CLOUDDQ_TARGET_BIGQUERY_TABLE="<project-id>.<dataset-id>.<table-id>"
python3 clouddq_executable.zip \
    ALL \
    configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
    --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}"
```

To see the resulting Data Quality validation summary statistics in the BigQuery table `$CLOUDDQ_TARGET_BIGQUERY_TABLE`, run:
```bash
#!/bin/bash
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_TARGET_BIGQUERY_TABLE="<project-id>.<dataset-id>.<table-id>"
echo "select * from \`${CLOUDDQ_TARGET_BIGQUERY_TABLE}\`" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql
```

To see the BigQuery SQL logic generated by CloudDQ for each `Rule Binding` stored as a view in `--gcp_bq_dataset_id`, run:
```bash
#!/bin/bash
export CLOUDDQ_BIGQUERY_DATASET=clouddq
bq show --view ${CLOUDDQ_BIGQUERY_DATASET}.T3_DQ_1_EMAIL_DUPLICATE
```

## Tutorials



## Example Use Cases



## Deployment Best Practices

### Authentication

CloudDQ supports the follow methods for authenticating to GCP:
1. OAuth via locally discovered [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/best-practices-applications#overview_of_application_default_credentials) if only the arguments `--gcp_project_id`, `--gcp_bq_dataset_id`, and `--gcp_region_id` are provided.
2. Using an [exported JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) if the argument `--gcp_service_account_key_path` is provided.
3. [Service Account impersonation](https://cloud.google.com/iam/docs/impersonating-service-accounts) via a source credentials if the argument `--gcp_impersonation_credentials` is provided. The source credentials can be obtained from either `--gcp_service_account_key_path` or from the local ADC credentials.



## Example Dashboards

