# Cloud Data Quality Engine

[![beta](https://badgen.net/badge/status/beta/1E90FF)](https://badgen.net/badge/status/beta/1E90FF)
[![build-test status](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml/badge.svg)](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Introductions

`CloudDQ` is a cloud-native, declarative, and scalable Data Quality validation Command-Line Interface (CLI) application for Google BigQuery.

It takes as input Data Quality validation tests defined using a flexible and reusable YAML configurations language. For each `Rule Binding` definition in the YAML configs, `CloudDQ` creates a corresponding SQL view in your Data Warehouse. It then executes the view and collects the data quality validation outputs into a summary table for reporting and visualization.

`CloudDQ` currently supports in-place validation of BigQuery data.

**Note:** This project is currently in beta status and may still change in breaking ways.

## License

CloudDQ is licensed under the Apache License version 2.0. This is not an official Google product.

## Contributions

We welcome all community contributions, whether by opening Github Issues, updating documentations, or updating the code directly. Please consult the [contribution guide](CONTRIBUTING.md) and [development guide](./README.md#Development) for details on how to contribute. 

Before opening a pull request to suggest a feature change, please open a Github Issue to discuss the use-case and feature proposal with the project maintainers.

## Declarative Data Quality Configs

### Rule Bindings
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
Each `rule_binding` must define one of the field `entity_id` or `entity_uri` and all of the fields `column_id`, `row_filter_id`, and `rule_ids`. `entity_id` refers to the table being validated, `entity_uri` is the URI path in a remote metadata registry that references the table being validated, `column_id` refers to the column in the table to be validated, `row_filter_id` refers to a filter condition to select rows in-scope for validation, and `rule_ids` refers to the list of data quality validation rules to apply on the selected column and rows.

If `incremental_time_filter_column_id` is set to a monotonically-increasing timestamp column in an append-only table, `CloudDQ` on each run will only validate rows where the timestamp specified in `incremental_time_filter_column_id` is higher than the timestamp of the last run. This allows CloudDQ to perform incremental validation without scanning the entire table everytime. If `incremental_time_filter_column_id` is not set, `CloudDQ` will validate all rows matching the `row_filter_id` on each run.

Under the `metadata` config, you may add any key-value pairs that will be added as a JSON on each DQ summary row output. For example, this can be the team responsible for a `rule_binding`, the table type (raw, curated, reference). The JSON allows custom aggregations and drilldowns over the summary data.

On each run, CloudDQ converts each `rule_binding` into a SQL script, create a corresponding [view](https://cloud.google.com/bigquery/docs/views) in BigQuery, and aggregate the validation results summary per `rule_binding` and `rule` for each CloudDQ run into a DQ Summary Statistics table in a BigQuery table specified in the CLI argument `--target_bigquery_summary_table`.

You can then use any dashboarding solution such as Data Studio or Looker to visualize the DQ Summary Statistics table, or use the DQ Summary Statistics table for monitoring and alerting purposes.

### Rule Dimensions
**Rule Dimensions**: defines the allowed list of Data Quality rule dimensions that a Data Quality `Rule` can define in the corresponding `dimension` field.

```yaml
rule_dimensions:
  - consistency
  - correctness
  - duplication
  - completeness
  - conformance
  - integrity
```
This list can be customized as is required. CloudDQ throws an error when parsing the YAML if it finds a `rule` with a `dimension` value that is not one of the allowed values.

### Rules
**Rules**: Defines reusable sets of validation logic for data quality.
```yaml
rules:
  NOT_NULL_SIMPLE:
    rule_type: NOT_NULL
    dimension: completeness

  NOT_BLANK:
    rule_type: NOT_BLANK
    dimension: completeness

  REGEX_VALID_EMAIL:
    rule_type: REGEX
    dimension: conformance
    params:
      pattern: |-
        ^[^@]+[@]{1}[^@]+$

  CUSTOM_SQL_LENGTH_LE_30:
    rule_type: CUSTOM_SQL_EXPR
    dimension: correctness
    params:
      custom_sql_expr: |-
        LENGTH( $column ) <= 30

  VALUE_ZERO_OR_POSITIVE:
    rule_type: CUSTOM_SQL_EXPR
    dimension: correctness
    params:
      custom_sql_expr: |-
        $column >= 0

  CORRECT_CURRENCY_CODE:
    rule_type: CUSTOM_SQL_EXPR
    dimension: conformance
    params:
      custom_sql_expr: |-
        $column in (select distinct currency_code from `<reference_dataset-id>.currency_codes`)

  FOREIGN_KEY_VALID:
    rule_type: CUSTOM_SQL_EXPR
    dimension: consistency
    params:
      custom_sql_expr: |-
        $column in (select distinct foreign_key from `<dataset-id>.customer_id`)

  NO_DUPLICATES_IN_COLUMN_GROUPS:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: duplication
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
We will add more out-of-the-box rule types such as `NOT_NULL`, `NOT_BLANK`, and `REGEX` over time. For the time being, most data quality rule can be defined as SQL using either `CUSTOM_SQL_EXPR` or `CUSTOM_SQL_STATEMENT` rule types.

`CUSTOM_SQL_EXPR` will apply the SQL expression logic defined in `custom_sql_expr` to every record and flag any row where `custom_sql_expr` evaluated to False as a failure. `CUSTOM_SQL_EXPR` rules are meant for record-level validation and will report success/failure counts at the record level. 

`CUSTOM_SQL_STATEMENT` is meant for set-level validation and instead accepts a complete SQL statement in the field `custom_sql_statement`. If the SQL statement returns 0 rows, `CloudDQ` considers the rule a success. If the SQL statement returns any row, `CloudDQ` will report the rule as a failure and include the returned row counts in the validation summary.

For both `CUSTOM_SQL_EXPR` and `CUSTOM_SQL_STATEMENT`, the templated parameter `$column` in the `custom_sql_expr` and `custom_sql_statement` blocks will be substituted with the `column_id` specified in each `rule_binding`. `CUSTOM_SQL_STATEMENT` additionally supports custom parameterized variables in the `custom_sql_arguments` block. These parameters can be bound separately in each `rule_binding`, allowing `CUSTOM_SQL_STATEMENT` to be defined once to be reused across multiple `rule_binding`s with different input arguments.

The SQL code block in `CUSTOM_SQL_STATEMENT` rules must include the string `from data` in the `custom_sql_statement` block, i.e. it must validate data returned by a `data` common table expression (CTE). The common table expression `data` contains rows returned once all `row_filters` and incremental validation logic have been applied.

`CloudDQ` reports validation summary statistics for each `rule_binding` and `rule` by appending to the target BigQuery table specified in the CLI argument `--target_bigquery_summary_table`. Record-level validation statistics are captured the in columns `success_count`, `success_percentage`, `failed_count`, `failed_percentage`, `null_count`, `null_percentage`. The rule type `NOT_NULL` reports the count of `NULL`s present in the input `column-id` in the columns `failed_count`, `failed_percentage`, and always set the columns `null_count` and `null_percentage` to `NULL`. `CUSTOM_SQL_STATEMENT` rules do not report record-level validation statistics and therefore will set the content of the columns `success_count`, `success_percentage`, `failed_count`, `failed_percentage`, `null_count`, `null_percentage` to `NULL`.

Set-level validation results for `CUSTOM_SQL_STATEMENT` rules are captured in the columns `complex_rule_validation_errors_count` and `complex_rule_validation_success_flag`. `complex_rule_validation_errors_count` contains the count of rows returned by the `custom_sql_statement` block. `complex_rule_validation_success_flag` is set to `TRUE` if `complex_rule_validation_errors_count` is equals to 0, `FALSE` if `complex_rule_validation_errors_count` is larger than 0, and `NULL` for all other rule types that are not `CUSTOM_SQL_STATEMENT`.

### Filters
**Filters**: Defines how each `Rule Binding` can be filtered. The content of the `filter_sql_expr` field will be inserted into a SQL `WHERE` clause for filtering your data to the rows for validation.
```yaml
row_filters:
  NONE:
    filter_sql_expr: |-
      True

  DATA_TYPE_EMAIL:
    filter_sql_expr: |-
      contact_type = 'email'

 INTERNATIONAL_ITEMS:
   filter_sql_expr: |-
      REGEXP_CONTAINS(item_id, 'INTNL')

LAST_WEEK:
   filter_sql_expr: |-
      date(last_modified_timestamp) >= DATE_SUB(current_date(), INTERVAL 1 WEEK)

```

### Entities
**Entities**: defines the target data tables as validation target.
```yaml
entities:
  TEST_TABLE:
    source_database: BIGQUERY
    table_name: contact_details
    dataset_name: <your_bigquery_dataset_id>
    project_name: <your_gcp_project_id>
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
Entities configurations contain details of the table to be validated, including its source database, connnection settings, and column configurations.

An example entity configurations is provided at `configs/entities/test-data.yml`. The data for the BigQuery table `contact_details` referred in this config can can be found in `tests/data/contact_details.csv`.

You can load this data into BigQuery using the `bq load` command (the [`bq` CLI ](https://cloud.google.com/bigquery/docs/bq-command-line-tool) is installed as part of the [`gcloud` SDK](https://cloud.google.com/sdk/docs/install)):

```bash
#!/bin/bash
# Create a BigQuery Dataset in a region of your choice and load data
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
# Fetch the example csv file
curl -LO https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-data-quality/main/tests/data/contact_details.csv
# Create BigQuery Dataset. Skip this step if `CLOUDDQ_BIGQUERY_DATASET` already exists
bq --location=${CLOUDDQ_BIGQUERY_REGION} mk --dataset ${GOOGLE_CLOUD_PROJECT}:${CLOUDDQ_BIGQUERY_DATASET}
# Load sample data to the dataset
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details contact_details.csv
```

Ensure you have sufficient IAM privileges to create BigQuery datasets and tables in your project.

If you are testing CloudDQ with the provided configs, ensure you update the `<your_gcp_project_id>` field with the [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) and the `<your_bigquery_dataset_id>` field with the BigQuery dataset containing the `contact_details` table.

**Entity URI**: defined a URI to look up an existing table entity in a remote metadata registry such as Dataplex.
```yaml
rule_bindings:
 TRANSACTIONS_UNIQUE:
   entity_uri: dataplex://projects/<project-id>/locations/<region-id>/lakes/<lake-id>/zones/<zone-id>/entities/<entity-id> # replace variables in <angle brackets> with your own configs
   column_id: id
   row_filter_id: NONE
   rule_ids:
     - NO_DUPLICATES_IN_COLUMN_GROUPS:
         column_names: "id"
```
Instead of manually specifying the `entities` configuration to be referenced in the `entity_id` field of each `Rule Binding`, you can specify an `entity_uri` containing the lookup path to a remote metadata registry referencing the table entity to be validated. This omit the need to define the `entities` configurations directly.

`entity_uri` currently supports looking up BigQuery tables using Dataplex Metadata API. The BigQuery dataset containing this table must be registered as a Dataplex Asset and Dataplex Discovery Jobs must have already discovered the table and created a corresponding Dataplex `entity_id`. The Dataplex `entity_id` path can then be referenced with the URI scheme `dataplex://` in the field `entity_uri` inside a `Rule Binding` for `CloudDQ` to automatically look-up the `entity_id` using Dataplex Metadata API to resolve to the table name to be validated.

### Metadata Registry Defaults
**Metadata Registry Defaults**: define default `entity_uri` values for each scheme.
```yaml
metadata_registry_defaults:
 dataplex: # replace variables in <angle brackets> with your own configs
   projects: <project-id> 
   locations: <region-id> 
   lakes: <lake-id>
   zones: <zone-id>

rule_bindings:
 TRANSACTIONS_UNIQUE:
   entity_uri: dataplex://entities/<entity-id> # omitting projects/locations/lakes from uri path to use the default values specified in metadata_registry_defaults
   column_id: id
   row_filter_id: NONE
   rule_ids:
     - NO_DUPLICATES_IN_COLUMN_GROUPS:
         column_names: "id"

 TRANSACTION_AMOUNT_VALID:
   entity_uri: dataplex://zones/<override-zone-id>/entities/<entity-id> # override the default zones values specified in metadata_registry_defaults
   column_id: amount
   row_filter_id: NONE
   rule_ids:
     - VALUE_ZERO_OR_POSITIVE
```
Instead of providing the full URI path for every `Rule Binding`, you can define the default values you would like to use in each `entity_uri` scheme (e.g. `dataplex://`) under the `metadata_registry_defaults:` YAML node.

Once `metadata_registry_defaults` is defined, you can omit the corresponding variable in the `entity_uri` to use the default values specified when resolving each `entity_uri` to a table name. You can additionally override the default by providing the variable directly in the `entity_uri`.

## Usage Guide

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

## Authentication

CloudDQ supports the follow methods for authenticating to GCP:
1. OAuth via locally discovered [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/best-practices-applications#overview_of_application_default_credentials) if only the arguments `--gcp_project_id`, `--gcp_bq_dataset_id`, and `--gcp_region_id` are provided.
2. Using an [exported JSON service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) if the argument `--gcp_service_account_key_path` is provided.
3. [Service Account impersonation](https://cloud.google.com/iam/docs/impersonating-service-accounts) via a source credentials if the argument `--gcp_impersonation_credentials` is provided. The source credentials can be obtained from either `--gcp_service_account_key_path` or from the local ADC credentials.

## Development

CloudDQ is currently only tested to run on `Ubuntu`/`Debian` linux distributions. It may not work properly on other OS such as `MacOS`, `Windows`/`cygwin`, or `CentOS`/`Fedora`/`FreeBSD`, etc...

For development or trying out CloudDQ, we recommend using either [Cloud Shell](https://cloud.google.com/shell/docs/launching-cloud-shell-editor) or a [Google Cloud Compute Engine VM](https://cloud.google.com/compute) with the [Debian 11 OS distribution](https://cloud.google.com/compute/docs/images/os-details#debian).

### Dependencies

* make: https://www.gnu.org/software/make/manual/make.html
* golang (for building [bazelisk](https://github.com/bazelbuild/bazelisk)): https://golang.org/doc/install
* Python 3.8.x or 3.9.x: https://wiki.python.org/moin/BeginnersGuide/Download
* gcloud SDK (for interacting with GCP): https://cloud.google.com/sdk/docs/install

From a `Ubuntu`/`Debian` machine, install the above dependencies by running the following script:

```bash
#!/bin/bash
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
source scripts/install_development_dependencies.sh
```

Building CloudDQ requires the command `python3` to point to a Python Interterpreter version 3.8.x or 3.9.x. To install the correct Python version, please refer to the script `scripts/poetry_install.sh` for an interactive installation or `scripts/install_python3.sh` for a non-interactive installation intended for automated build/test processes. 

### Building a self-contained executable from source

After installing all dependencies and building it, you can clone the latest version of the code and build it by running the following commands:

```bash
#!/bin/bash
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
cd cloud-data-quality
make build
```

We provide a `Makefile` with common development steps such as `make build` to create the artifact, `make test` to run tests, and `make lint` to apply linting over the project code.

The `make build` command will fetch `bazel` and build the project into a self-contained Python zip executable called `clouddq_patched.zip` located in the current path.

You can then run the resulting zip artifact by passing it into the same Python interpreter version used to build the executable (this will show the help text):

```bash
#!/bin/bash
python3 clouddq_patched.zip --help
```

This step will take a few minutes to complete. Once completed for the first time, the artifacts will be cached and subsequent builds will be much faster.

The executable Python zip is not cross-platform compatible. You will need to build the executable separately for each of your target platforms. e.g. an artifact built using a machine running Ubuntu-18 and Python version 3.9.x will not work when transfered to another machine with Ubuntu-18 and Python version 3.8.x or another machine with Debian-11 and Python version 3.9.x.

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
export GOOGLE_CLOUD_PROJECT=$(gcloud config get-value project)
export CLOUDDQ_BIGQUERY_REGION=EU
export CLOUDDQ_BIGQUERY_DATASET=clouddq
export CLOUDDQ_TARGET_BIGQUERY_TABLE="<project-id>.<dataset-id>.<table-id>"
bin/bazelisk run //clouddq:clouddq -- \
  T2_DQ_1_EMAIL \
  $(pwd)/configs \
  --gcp_project_id="${GOOGLE_CLOUD_PROJECT}" \
  --gcp_bq_dataset_id="${CLOUDDQ_BIGQUERY_DATASET}" \
  --gcp_region_id="${CLOUDDQ_BIGQUERY_REGION}" \
  --target_bigquery_summary_table="${CLOUDDQ_TARGET_BIGQUERY_TABLE}"  
```

Note that `bazel run` execute the code in a sandbox, therefore non-absolute paths will be relative to the sandbox path not the current path. Ensure you are passing absolute paths to the command line arguments, e.g. pass in `$(pwd)/configs` instead of 'configs'.


### Run tests and linting

To run all tests:

```bash
#!/bin/bash
make test
```

For integration testing, you must first set the environment variables outlined in `set_environment_variables.sh` before running  `make test`. For example:

```bash
#!/bin/bash
cp set_environment_variables.sh setenvs.sh
# Manually edit `setenvs.sh` to use your project configurations.
source setenvs.sh && make test
```

To run a particular test:

```bash
#!/bin/bash
make test test_templates
```

By default, running integration tests with Dataplex are skipped. To enable these tests, ensure the correct environments are set and run:

```bash
#!/bin/bash
make build  # create the relevant zip artifacts to be used in tests
make test -- --run-dataplex
```

To apply linting:

```bash
#!/bin/bash
make lint
```

To run build and tests on clean OS images, install [cloud-build-local](https://cloud.google.com/build/docs/build-debug-locally) and run:

```bash
#!/bin/bash
./setenvs.sh && ./scripts/cloud-build-local.sh  2>&1 | tee build.log
```

This will set the environment variables required for the run and pipe the run logs to a file called `build.log`.

This will run as your gcloud ADC credentials. If you are running on a GCE VM, ensure the Compute Engine service account has the access scope to use all GCP APIs.

To run cloud-build-local in dry-run mode:

```bash
#!/bin/bash
bash scripts/cloud-build-local.sh --dry-run
```

## Troubleshooting

### 1. Cannot find shared library dependencies on system
If running `make build` fails due to missing shared library dependencies (e.g. `_ctypes` or `libssl`), try running the below steps to install them, then clear the bazel cache with `make clean` and retry.

#### `Ubuntu`/`Debian`:

```bash
#!/bin/bash
source scripts/install_development_dependencies.sh
```

### 2. Wrong `glibc` version
If you encounter the following error when running the executable Python zip on a different machine to the one you used to build your zip artifact:

```
/lib/x86_64-linux-gnu/libm.so.6: version `GLIBC_2.xx' not found
```

This suggests that the `glibc` version on your target machine is incompatible with the version on your build machine. Resolve this by rebuilding the zip on machine with identical `glibc` version (usually this means the same OS version) as on your target machine, or vice versa.

## Feedback / Questions

For any feedback or questions, please feel free to get in touch  at `clouddq` at `google.com`.

