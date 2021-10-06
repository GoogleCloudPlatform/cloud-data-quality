# Cloud Data Quality Engine

[![alpha](https://badgen.net/badge/status/alpha/d8624d)](https://badgen.net/badge/status/alpha/d8624d)
[![build-test status](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml/badge.svg)](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Introductions

`CloudDQ` is a cloud-native, declarative, and scalable Data Quality validation Command-Line Interface (CLI) application for Google BigQuery.

It takes as input Data Quality validation tests defined using a flexible and reusable YAML configurations language. For each `Rule Binding` definition in the YAML configs, `CloudDQ` creates a corresponding SQL view in your Data Warehouse. It then executes the view and collects the data quality validation outputs into a summary table for reporting and visualization.

`CloudDQ` currently supports in-place validation of BigQuery data.

**Note:** This project is currently in alpha status and may change in breaking ways.

### Declarative Data Quality Configs

#### Rule Bindings
**Rule Bindings**:
Defines a single Data Quality validation routine.
Each value declared in `entity_id`, `column_id`, `filter_id`, and `rule_id`
is a lookup key for the more detailed configurations that must be defined in their respective configurations files.
```yaml
rule_bindings:
  T2_DQ_1_EMAIL:
    entity_id: TEST_TABLE
    column_id: VALUE
    row_filter_id: DATA_TYPE_EMAIL
    incremental_time_filter_column_id: TS
    rule_ids:
      - NOT_NULL_SIMPLE
      - REGEX_VALID_EMAIL
      - CUSTOM_SQL_LENGTH_LE_30
    metadata:
      team: team-2

  T3_DQ_1_EMAIL_DUPLICATE:
    entity_id: TEST_TABLE
    column_id: VALUE
    row_filter_id: DATA_TYPE_EMAIL
    rule_ids:
      - NO_DUPLICATES_IN_COLUMN_GROUPS:
          column_names: "value"
      - NOT_NULL_SIMPLE
    metadata:
      team: team-3
```
Each `rule_binding` must define the fields `entity_id`, `column_id`, `row_filter_id`, and `rule_ids`. `entity_id` refers to the table being validated, `column_id` refers to the column in the table to be validated, `row_filter_id` refers to a filter condition to select rows in-scope for validation, and `rule_ids` refers to the list of data quality validation rules to apply on the selected column and rows.

If `incremental_time_filter_column_id` is defined, `CloudDQ` will only validate rows with a timestamp higher than the last run timestamp on each run, otherwise it will validate all rows matching the `row_filter_id` on each run.

Under the `metadata` config, you may add any key-value pairs that will be added as a JSON on each DQ summary row output. For example, this can be the team responsible for a `rule_binding`, the table type (raw, curated, reference). The JSON allows custom aggregations and drilldowns over the summary data.

On each run, CloudDQ converts each `rule_binding` into a SQL script, create a corresponding [view](https://cloud.google.com/bigquery/docs/views) in BigQuery, and aggregate the validation results summary per `rule_binding` and `rule` for each CloudDQ run into a DQ Summary Statistics table in BigQuery. This table is called called `dq_summary`, and is automatically created by CloudDQ in the BigQuery dataset defined in the argument `--gcp_bq_dataset_id`.

You can then use any dashboarding solution such as Data Studio or Looker to visualize the DQ Summary Statistics table, or use the DQ Summary Statistics table for monitoring and alerting purposes.

#### Rules
**Rules**: Defines reusable sets of validation logic for data quality.
```yaml
rules:
  NOT_NULL_SIMPLE:
    rule_type: NOT_NULL

  REGEX_VALID_EMAIL:
    rule_type: REGEX
    params:
      pattern: |-
        ^[^@]+[@]{1}[^@]+$

  CUSTOM_SQL_LENGTH_LE_30:
    rule_type: CUSTOM_SQL_EXPR
    params:
      custom_sql_expr: |-
        LENGTH( $column ) <= 30

  NO_DUPLICATES_IN_COLUMN_GROUPS:
    rule_type: CUSTOM_SQL_STATEMENT
    params:
      custom_sql_arguments:
        - column_names
      custom_sql_statement: |-
        select a.*
        from data a
        inner join (
          select
            $column_names
          from data
          group by $column_names
          having count(*) > 1
        ) duplicates
        using ($column_names)
```
We will add more default rule types over time. For the time being, most data quality rule can be defined as SQL using either `CUSTOM_SQL_EXPR` or `CUSTOM_SQL_STATEMENT`. `CUSTOM_SQL_EXPR` will flag any row that `custom_sql_expr` evaluated to False as a failure. `CUSTOM_SQL_STATEMENT` will flag any value returned by the whole statement as failures.

`CloudDQ` will substitute the string `$column` with the YAML config value provided in the field `column_id` of each `rule_binding` `CUSTOM_SQL_STATEMENT` additionally supports custom parameterized variables in `custom_sql_arguments` that can be defined separately in each `rule_binding`.

When using `CUSTOM_SQL_STATEMENT`, the table `data` contains rows returned once all `row_filters` and incremental validation logic have been applied. We recommend simply selecting from `data` in `CUSTOM_SQL_STATEMENT` instead of trying to apply your own templating logic to define the target table for validation.

#### Filters
**Filters**: Defines how each `Rule Binding` can be filtered
```yaml
row_filters:
  NONE:
    filter_sql_expr: |-
      True

  DATA_TYPE_EMAIL:
    filter_sql_expr: |-
      contact_type = 'email'
```

#### Entities
**Entities**: defines the target data tables as validation target.
```yaml
entities:
  TEST_TABLE:
    source_database: BIGQUERY
    table_name: contact_details
    dataset_name: <your_dataset_id>
    project_name: <your_project_id>
    environment_override:
      TEST:
        environment: test
        override:
          database_name: <your_project_id>
          instance_name: <your_project_id>
    columns:
      ROW_ID:
        name: row_id
        data_type: STRING
        description: |-
          unique identifying id
      CONTACT_TYPE:
        name: contact_type
        data_type: STRING
        description: |-
          contact detail type
      VALUE:
        name: value
        data_type: STRING
        description: |-
          contact detail
      TS:
        name: ts
        data_type: DATETIME
        description: |-
          updated timestamp
```

An example entity configurations is provided at `configs/entities/test-data.yml`. The data for the BigQuery table `contact_details` referred in this config can can be found in `tests/data/contact_details.csv`.

You can load this data into BigQuery using the `bq load` command (the [`bq` CLI ](https://cloud.google.com/bigquery/docs/bq-command-line-tool) is installed as part of the [`gcloud` SDK](https://cloud.google.com/sdk/docs/install)):

```bash
#!/bin/bash
# Create a BigQuery Dataset in a region of your choice and load data
export PROJECT_ID=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
# Fetch the example csv file
curl -LO https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-data-quality/main/tests/data/contact_details.csv
# Create BigQuery Dataset. Skip this step if `CLOUDDQ_BIGQUERY_DATASET` already exists
bq --location=${CLOUDDQ_BIGQUERY_REGION} mk --dataset ${PROJECT_ID}:${CLOUDDQ_BIGQUERY_DATASET}
# Load sample data to the dataset
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details contact_details.csv
```

Ensure you have sufficient IAM privileges to create BigQuery datasets and tables in your project.

If you are testing CloudDQ with the provided configs, ensure you update the `<your_project_id>` field with the [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) and the `<your_dataset_id>` field with the BigQuery dataset containing the `contact_details` table.

## Usage Guide

### Overview

CloudDQ is a Command-Line Interface (CLI) application. It takes as input YAML Data Quality configurations, generates and executes SQL code in BigQuery using provided connection configurations, and writes the resulting Data Quality summary statistics to a BigQuery table of your choice.

#### System Requirements

CloudDQ is currently only tested to run on `Ubuntu`/`Debian` linux distributions. It may not work properly on other OS such as `MacOS`, `Windows`/`cygwin`, or `CentOS`/`Fedora`/`FreeBSD`, etc...

For development or trying out CloudDQ, we recommend using either [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell-editor) or a [Google Cloud Compute Engine VM](https://cloud.google.com/compute) with the [Ubuntu 18.04 OS distribution](https://cloud.google.com/compute/docs/images/os-details#ubuntu_lts).

CloudDQ requires a Python 3.8 interpreter. If you are using the pre-built artifact, the Python interpreter is bundled into the zip so it can be executed using any Python verion.

#### Using Pre-Built Executable

The simplest way to run CloudDQ is to use one of the pre-built executable provided in the Github releases page: https://github.com/GoogleCloudPlatform/cloud-data-quality/releases

For example, from [Cloud Shell](https://shell.cloud.google.com/?show=ide%2Cterminal), you can download the executable with the following commands:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.3.1"
wget -O clouddq_executable.zip https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v"${CLOUDDQ_RELEASE_VERSION}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}"_linux-amd64.zip
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
export CLOUDDQ_RELEASE_VERSION="0.3.1"
git clone -b "v${CLOUDDQ_RELEASE_VERSION}" https://github.com/GoogleCloudPlatform/cloud-data-quality.git
```

Then change directory to the git project and get the pre-built executable from Github:

```bash
#!/bin/bash
export CLOUDDQ_RELEASE_VERSION="0.3.1"
cd cloud-data-quality
wget -O clouddq_executable.zip https://github.com/GoogleCloudPlatform/cloud-data-quality/releases/download/v"${CLOUDDQ_RELEASE_VERSION}"/clouddq_executable_v"${CLOUDDQ_RELEASE_VERSION}"_linux-amd64.zip
```

Run the following command to authenticate to GCP using application-default credentials. The command will prompt you to login and provide a verification code back into the console.
```bash
#!/bin/bash
gcloud auth application-default login
```

Then locate your [GCP project ID](https://support.google.com/googleapi/answer/7014113) and set it to a local variable for usage later. We will also configure `gcloud` to use this GCP Project ID.

```bash
#!/bin/bash
export PROJECT_ID="<your_GCP_project_id>"
gcloud config set project ${PROJECT_ID}
```

Now we'll create a new dataset with a custom name in a location of our choice and load some sample data into it:

```bash
#!/bin/bash
# Create a BigQuery Dataset in a region of your choice and load data
export PROJECT_ID=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
# Create BigQuery Dataset. Skip this step if `CLOUDDQ_BIGQUERY_DATASET` already exists
bq --location=${CLOUDDQ_BIGQUERY_REGION} mk --dataset ${PROJECT_ID}:${CLOUDDQ_BIGQUERY_DATASET}
# Load sample data to the dataset
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details tests/data/contact_details.csv
```

Before running CloudDQ, we need to edit the table configurations in `configs/entities/test-data.yml` to use our Project ID and BigQuery dataset ID.

```bash
#!/bin/bash
export PROJECT_ID=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
sed -i s/\<your_gcp_project_id\>/${PROJECT_ID}/g configs/entities/test-data.yml
sed -i s/dq_test/${CLOUDDQ_BIGQUERY_DATASET}/g configs/entities/test-data.yml
```

Using the same Project ID, GCP Region, and BigQuery dataset ID as defined before, we will attempt to execute two `Rule Binding`s with the unique identifers `T2_DQ_1_EMAIL` and `T3_DQ_1_EMAIL_DUPLICATE` from the `configs` directory containing the complete YAML configurations:

```bash
#!/bin/bash
export PROJECT_ID=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
python3 clouddq_executable.zip \
    T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE \
    configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}"
```

By running this CLI command, `CloudDQ` will:
1. validate that the YAML configs are valid, i.e. every `Rule`, `Entity`, and `Row Filter` referenced in each `Rule Binding` exists and there are no duplicated configuration ID. CloudDQ will attempt to parse all files with extensions `yml` and `yaml` discovered in a path and use the top-level keys (e.g. `rules`, `rule_bindings`, `row_filters`, `entities`) to determine the config type. Each configuration item must have a globally unique identifier in the `config` path.
2. validate that the dataset `--gcp_bq_dataset_id` is located in the region `--gcp_region_id`
3. generate BigQuery SQL code for each `Rule Binding` and validate that it is valid BigQuery SQL using BigQuery dry-run feature
4. create a BigQuery view for each generated BigQuery SQL `Rule Binding` in the BigQuery dataset specified in `--gcp_bq_dataset_id`
5. create a BigQuery job to execute all `Rule Binding` SQL views. The BigQuery jobs will be created in the GCP project specified in `--gcp_project_id` and GCP region specified in `--gcp_region_id`
6. aggregate the validation outcomes and write the Data Quality summary results into a table called `dq_summary` in the BigQuery dataset specified in `--gcp_bq_dataset_id`

To execute all `Rule Binding`s discovered in the config directory, use `ALL` as the `RULE_BINDING_IDS`:

```bash
#!/bin/bash
python3 clouddq_executable.zip \
    ALL \
    configs \
    --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
    --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
    --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}"
```

To see the resulting Data Quality validation summary statistics in the BigQuery table `dq_summary`, run:
```bash
#!/bin/bash
echo "select * from \`${PROJECT_ID}\`.${CLOUDDQ_BIGQUERY_DATASET}.dq_summary" | bq query --location=${CLOUDDQ_BIGQUERY_REGION} --nouse_legacy_sql
```

To see the BigQuery SQL logic generated by CloudDQ for each `Rule Binding`, run:
```bash
#!/bin/bash
bq show --view clouddq.T3_DQ_1_EMAIL_DUPLICATE
```

## Authentication

CloudDQ supports the follow methods for authenticating to GCP:
1. OAuth via locally discovered [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/best-practices-applications#overview_of_application_default_credentials) if only the arguments `--gcp_project_id`, `--gcp_bq_dataset_id`, and `--gcp_region_id` are provided.
2. Using an [exported JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) if the argument `--gcp_service_account_key_path` is provided.
3. [Service Account impersonation](https://cloud.google.com/iam/docs/impersonating-service-accounts) via a source credentials if the argument `--gcp_impersonation_credentials` is provided. The source credentials can be obtained from either `--gcp_service_account_key_path` or from the local ADC credentials.

## Development

CloudDQ is currently only tested to run on `Ubuntu`/`Debian` linux distributions. It may not work properly on other OS such as `MacOS`, `Windows`/`cygwin`, or `CentOS`/`Fedora`/`FreeBSD`, etc...

For development or trying out CloudDQ, we recommend using either [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell-editor) or a [Google Cloud Compute Engine VM](https://cloud.google.com/compute) with the [Ubuntu 18.04 OS distribution](https://cloud.google.com/compute/docs/images/os-details#ubuntu_lts).

### Dependencies

* make: https://www.gnu.org/software/make/manual/make.html
* golang (for building [bazelisk](https://github.com/bazelbuild/bazelisk)): https://golang.org/doc/install
* Python 3: https://wiki.python.org/moin/BeginnersGuide/Download
* gcloud SDK (for interacting with GCP): https://cloud.google.com/sdk/docs/install
* sandboxfs: https://github.com/bazelbuild/sandboxfs/blob/master/INSTALL.md

From a `Ubuntu`/`Debian` machine, install the above dependencies by running the following script:

```bash
#!/bin/bash
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
bash scripts/install_development_dependencies.sh
```

### Building a self-contained executable from source

After installing all dependencies and building it, you can clone the latest version of the code and build it by running the following commands:

```bash
#!/bin/bash
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
cd cloud-data-quality
make build
```

We provide a `Makefile` with common development steps such as `make build` to create the artifact, `make test` to run tests, and `make lint` to apply linting over the project code.

The `make build` command will fetch `bazel` and build the project into a self-contained Python zip executable located in `bazel-bin/clouddq/clouddq_patched.zip`.

You can then run the resulting zip artifact by passing it into any Python interpreter (this will show the help text):

```bash
#!/bin/bash
python bazel-bin/clouddq/clouddq_patched.zip --help
```

As Bazel will fetch the Python interpreter as well as all of its dependencies, this step will take a few minutes to complete. Once completed for the first time, the artifacts will be cached and subsequent builds will be much faster.

The Python zip have been tested with Python versions `>=2.7.17` and `>=3.8.6`. As the zip contains a bundled python interpreter as well as all of `clouddq`'s dependencies, there is no need to ensure the python interpreter used to execute the zip has the project's Python dependencies installed. You may still need to install relevant Python dependencies by running:

```bash
#!/bin/bash
sudo apt-get update; sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

The executable Python zip is not cross-platform compatible. You will need to build the executable separately for each of your target platforms.

To speed up builds, you may want to update the [bazel cache](https://docs.bazel.build/versions/master/remote-caching.html#google-cloud-storage) in `.bazelrc` to a GCS bucket you have access to.
```
build --remote_cache=https://storage.googleapis.com/<your_gcs_bucket_name>
```

### Running the CLI from source without building

You may prefer to run the CLI directly without first building a zip executable. In which case, you can run `bazelisk run` to execute the main CLI.

First install Bazelisk into a local path:

```bash
#!/bin/bash
make bin/bazelisk
```

Then run the CloudDQ CLI using `bazelisk run` and pass in the CLI arguments after the `--`:

```bash
#!/bin/bash
bin/bazelisk run //clouddq:clouddq -- \
  T2_DQ_1_EMAIL \
  $(pwd)/configs \
  --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
  --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
  --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}"
```

Note that `bazel run` execute the code in a sandbox, therefore non-absolute paths will be relative to the sandbox path not the current path. Ensure you are passing absolute paths to the command line arguments such as `$(pwd)/configs`, `--gcp_service_account_key_path`, etc...

Additionally, you may want to run `source scripts/common.sh && confirm_gcloud_login` to check that your `gcloud` credentials are set up correctly.

### Run tests and linting

To run unit tests:
```
make test
```

To run a particular test:
```
make test test_templates
```

To apply linting:
```
make lint
```

## Troubleshooting

### 1. Cannot find shared library dependencies on system
If running `make build` fails due to missing shared library dependencies (e.g. `_ctypes` or `libssl`), try the below steps to install them, then clear the bazel cache with `make clean` and retry.

#### `Ubuntu`/`Debian`:

```bash
#!/bin/bash
sudo apt-get update; sudo apt-get install --no-install-recommends make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

### 2. Wrong `glibc` version
If you encounter the following error when running the executable Python zip on a different machine to the one you used to build your zip artifact:

```
/lib/x86_64-linux-gnu/libm.so.6: version `GLIBC_2.xx' not found
```

This suggests that the `glibc` version on your target machine is incompatible with the version on your build machine. Resolve this by rebuilding the zip on machine with identical `glibc` version (usually this means the same OS version) as on your target machine, or vice versa.

### 3. Failed to initiatize sandboxfs: Failed to get sandboxfs version from sandboxfs

This means you have not completed installing `sandboxfs` before running `make build`.

From a `Ubuntu`/`Debian` machine, install the above dependencies by running the following script:

```bash
#!/bin/bash
bash scripts/install_development_dependencies.sh
```

## Feedback / Questions

For any feedback or questions, please feel free to get in touch  at `clouddq` at `google.com`.

## License

CloudDQ is licensed under the Apache License version 2.0. This is not an official Google product.
