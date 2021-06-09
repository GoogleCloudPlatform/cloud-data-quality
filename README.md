# Cloud Data Quality Engine

[![alpha](https://badgen.net/badge/status/alpha/d8624d)](https://badgen.net/badge/status/alpha/d8624d)
[![build-test status](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml/badge.svg)](https://github.com/GoogleCloudPlatform/cloud-data-quality/actions/workflows/build-test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Introductions

`CloudDQ` is a cloud-native, declarative, and scalable Data Quality validation framework for Google BigQuery.

Data Quality validation tests can be defined using a flexible and reusable YAML configurations language. For each rule binding definition in the YAML configs, `CloudDQ` creates a corresponding SQL view in your Data Warehouse. It then executes the view and collects the data quality validation outputs into a summary table for reporting and visualization.

`CloudDQ` currently supports in-place validation of BigQuery data.

**Note:** This project is currently in alpha status and may change in breaking ways.

### Declarative Data Quality Configs

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
      brand: one

  T3_DQ_1_EMAIL_DUPLICATE:
    entity_id: TEST_TABLE
    column_id: VALUE
    row_filter_id: DATA_TYPE_EMAIL
    rule_ids:
      - NO_DUPLICATES_IN_COLUMN_GROUPS:
          column_names: "value"
      - NOT_NULL_SIMPLE
    metadata:
      brand: two
```
Each `rule_binding` must define the fields `entity_id`, `column_id`, `row_filter_id`, and `rule_ids`. If `incremental_time_filter_column_id` is defined, `CloudDQ` will only validate rows with a timestamp higher than the last run timestamp on each run, otherwise it will validate all rows matching the `row_filter_id` on each run.

Under the `metadata` config, you may add key-value pairs that will be added as a JSON on each DQ summary row output. The JSON allows custom aggregations and drilldowns over the summary data.

On each run, CloudDQ converts each `rule_binding` into a SQL script, create a corresponding [view](https://cloud.google.com/bigquery/docs/views) in BigQuery, and aggregate the validation results into a table called `dq_summary` at the location defined in the project's `profiles.yml`configurations.

You can then use any dashboarding solution such as Data Studio or Looker to visualize the DQ summary table for monitoring and alerting purposes.

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
        select
          $column_names
        from data
        group by $column_names
        having count(*) > 1
```
We will add more default rule types over time. For the time being, most data quality rule can be defined with SQL using either `CUSTOM_SQL_EXPR` or `CUSTOM_SQL_STATEMENT`. `CUSTOM_SQL_EXPR` will flag any row that `custom_sql_expr` evaluated to False as a failure. `CUSTOM_SQL_STATEMENT` will flag any value returned by the whole statement as failures.

`CloudDQ` will substitute the string `$column` with the YAML config value provided in `column_id`. `CUSTOM_SQL_STATEMENT` additionally supports custom parameterized variables in `custom_sql_arguments` that can be defined separately in each `rule_binding`.

When using `CUSTOM_SQL_STATEMENT`, the table `data` contains rows returned once all `row_filters` and incremental validation logic have been applied. We recommend simply selecting from `data` in `CUSTOM_SQL_STATEMENT` instead of trying to apply your own templating logic to define the target table for validation.

**Filters**: Defines how each rule binding can be filtered
```yaml
row_filters:
  NONE:
    filter_sql_expr: |-
      True

  DATA_TYPE_EMAIL:
    filter_sql_expr: |-
      contact_type = 'email'
```
**Entities**: defines the target data tables as validation target.
```yaml
entities:
  TEST_TABLE:
    source_database: BIGQUERY
    table_name: contact_details
    database_name: dq_test
    instance_name: <your_project_id>
    environment_override:
      TEST:
        environment: test
        override:
          database_name: <your_project_id>
          instance_name: <your_project_id>
    columns:
      KEY:
        name: key
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

An example entity configurations is provided at `configs/entities/test-data.yml`. The BigQuery table `contact_details` that is referred in this config can created using `dbt seed --profiles-dir=.` [details](#setting-up-`dbt`).

If you are testing CloudDQ with the provided configs, ensure you update the `<your_project_id>` field with the [GCP project ID](https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin) you are using in the `profiles.yml` file.

You can get the project ID of your project by running:

```
gcloud config get-value project
```

## Usage Guide

### Setting up `dbt`

The project uses [dbt](https://www.getdbt.com/) to execute SQL queries against BigQuery.

To set up `dbt`, you must configure connection profiles to a BigQuery project on GCP using a `profiles.yml` file, specifying the GCP `project` ID, BigQuery `dataset`, BigQuery job `location`, and authentication `method`. Within this same config file, you can define different target environment for `dev`, `test`, `prod`, etc... to be passed into CloudDQ's `environment_target` argument.

We recommend authenticating using `oauth` ([details](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#oauth-via-gcloud)), either 1) with your [application-default](https://google.aip.dev/auth/4110) credentials via `gcloud auth application-default login` ([details]((https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#oauth-via-gcloud))) for Development, or 2) with `impersonate_service_account` ([details](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#service-account-impersonation)) for Test/Production.

More information about dbt's `profiles.yml` configuration options for BigQuery can be found at https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile/.

You can start by copying the template at:
```bash
cp dbt/profiles.yml.template dbt/profiles.yml
```

Note that if you change the `profile` name in `profiles.yml.template` from `default` to something else, you will need to make the corresponding change to the config `profile` in `dbt_project.yml`.

The Data Quality validation results from each run will be collected into a table called `dq_summary` located at the `project` and `dataset` location in the `profile.yml` configs.

To create the test dataset used in the code's test-suites and in the following examples, run (after installing `dbt` in a Python virtualenv):
```
dbt seed --profiles-dir=.
```

Alternatively, you can use `bq load` instead of `dbt seed`:
```bash
export CLOUDDQ_BIGQUERY_REGION=EU # ensure this is the same as the 'location' config in your `profiles.yml` file.
export CLOUDDQ_BIGQUERY_DATASET=clouddq # ensure this is the same as the 'dataset' config in your `profiles.yml` file.
bq mk --location=${CLOUDDQ_BIGQUERY_REGION} ${CLOUDDQ_BIGQUERY_DATASET}
bq load --source_format=CSV --autodetect ${CLOUDDQ_BIGQUERY_DATASET}.contact_details dbt/data/contact_details.csv
```

Ensure you have sufficient IAM privileges to create BigQuery datasets and tables in your project.

### Installing

Ensure you have installed:
* Python version >3.8.6. We suggest using pyenv: https://github.com/pyenv/pyenv
* gcloud SDK (for interacting with GCP): https://cloud.google.com/sdk/docs/install

#### Installing from source

Clone the project:
```
git clone https://github.com/GoogleCloudPlatform/cloud-data-quality
```

Create a new python virtualenv:
```
python3 -m venv env
source env/bin/activate
```

Install `clouddq` from source:
```
python3 -m pip install .
```

You can then call the CLI by running:
```
python3 -m clouddq --help
```

### Usage

#### Testing the CLI with the default configurations

For a detailed, step-by-step walk through on how to test CloudDQ using the default configurations in this project, see [docs/getting-started-with-default-configs.md](docs/getting-started-with-default-configs.md).

#### Example CLI commands

Below is an example command to execute two rule bindings `T2_DQ_1_EMAIL` and `T3_DQ_1_EMAIL_DUPLICATE` from a path `configs` containing the complete YAML configurations:
```
python3 clouddq \
    T2_DQ_1_EMAIL,T3_DQ_1_EMAIL_DUPLICATE \
    configs \
    --metadata='{"test":"test"}' \
    --dbt_profiles_dir=dbt \
    --dbt_path=dbt \
    --environment_target=dev
```

To execute all rule bindings in a path containing the complete YAML configurations called `configs`, use `ALL` as the `RULE_BINDING_IDS`:
```
python3 clouddq \
    ALL \
    configs \
    --metadata='{"test":"test"}' \
    --dbt_profiles_dir=dbt \
    --dbt_path=dbt \
    --environment_target=dev
```

The CLI expects the paths for `dbt_profiles_dir`, `dbt_path`, and `environment_target` to be provided at runtime.

`dbt_profiles_dir` is the path to the `profiles.yml` config file containing the connection profile to BigQuery. You can additionally specify the `dbt` profile you want to use by passing the profile name to the input variable `environment_target`. You can find more details about configuring `dbt`'s `profiles.yml` file at [Setting up `dbt`](./README.md#setting-up-dbt).

The input variable `dbt_path` is where `CloudDQ` will stage the SQL views for each rule-bindings before creating them and aggregating the validation output into the `dq_summary` table. If a directory named `dbt` is not present at the path provided in the input argument, `CloudDQ` will create one using the default configurations as is provided in this git repository.

The content of `dbt_profiles_dir` and `dbt_path` can be customized to meet your needs. For example, you can:
1) customize the values returned in the `dq_summary` table by modifying `dbt/models/data_quality_engine/dq_summary.sql`,
2) calculate different summary scores in `dbt/models/data_quality_engine/main.sql`, or
3) create a new SQL model to write rows that failed a particular `rule_binding` from `dbt/models/rule_binding_views/` into a different table for reporting and remediation.

By default, `clouddq` does not write data from the original tables into any other table to avoid accidentally leaking PII.

`clouddq` CLI expects to find a local top-level `macros` directory, even if you cannot pass it in during runtime. This is why we recommend either installing `clouddq` by cloning the entire repository or by [building a self-contained Python executable with Bazel](#build-a-self-contained-python-executable-with-bazel).

### Development

#### Dependencies

* make: https://www.gnu.org/software/make/manual/make.html
* golang (for building [bazelisk](https://github.com/bazelbuild/bazelisk)): https://golang.org/doc/install
* Python 3: https://wiki.python.org/moin/BeginnersGuide/Download
* gcloud SDK (for interacting with GCP): https://cloud.google.com/sdk/docs/install
* sandboxfs: https://github.com/bazelbuild/sandboxfs/blob/master/INSTALL.md

The development commands provided in the `Makefile` have been tested to work on `debian` and `ubuntu`. They have not been tested on `mac-os`. There is currently no planned support for `windows`.

See `make` options at:
```
make help
```

Note that you may want to update the [bazel cache](https://docs.bazel.build/versions/master/remote-caching.html#google-cloud-storage) in `.bazelrc` to a GCS bucket you have access to.
```
build --remote_cache=https://storage.googleapis.com/<your_gcs_bucket_name>
```

#### Run CLI with Bazel

First install Bazelisk into a local path:
```
make bin/bazelisk
```

Then use `bazelisk run` to execute the main CLI:
```
bin/bazelisk run //clouddq:clouddq -- \
  T2_DQ_1_EMAIL \
  $(pwd)/configs \
  --metadata='{"test":"test"}' \
  --dbt_profiles_dir="$(pwd)" \
  --dbt_path="$(pwd)/dbt" \
  --environment_target=dev
```

As `bazel run` execute the code in a sandbox, non-absolute paths will be relative to the sandbox path not the current path. Ensure you are passing absolute paths to the command line arguments such as `dbt_profiles_dir`, `dbt_path`, etc...

Additionally, you may want to run `source scripts/common.sh && confirm_gcloud_login` to check that your `gcloud` credentials are set up correctly.

#### Run tests and linting

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

### Deployment

#### Build a self-contained Python executable with Bazel

You can use Bazel to build a self-contained executable Python zip file that includes a Python interpreter as well as all of the project dependencies such as `dbt`.

To build and executable Python zip file:

```
make build
```

The build step depends on `sandboxfs` in order to ensure the build artifact has as little dependencies on the host environment as possible. Ensure `sandboxfs` is installed by following the instructions at: https://github.com/bazelbuild/sandboxfs/blob/master/INSTALL.md

You can read more about Bazel sandboxing here: https://docs.bazel.build/versions/master/sandboxing.html

You can then run the resulting zip artifact by passing it into any Python interpreter (this will show the help text):

```
python bazel-bin/clouddq/clouddq_patched.zip --help
```

As Bazel will fetch the Python interpreter as well as all of its dependencies, this step will take a few minutes to complete. Once completed for the first time, the artifacts will be cached and subsequent builds will be much faster.

The Python zip have been tested with Python versions `>=2.7.17` and `>=3.8.6`. As the zip contains a bundled python interpreter as well as all of `clouddq`'s dependencies, there is no need to ensure the python interpreter used to execute the zip has its dependencies installed.

The executable Python zip is not cross-platform compatible. You will need to build the executable separately for each of your target platforms.

The Python zip includes the top-level `macros` directory, which will be hard-coded in the executable and cannot be changed at runtime.



#### Deploy Python Zip as a Dataproc Job

For a detailed, step-by-step walk through on how to deploy a CloudDQ job using [Dataproc](https://cloud.google.com/dataproc/docs/concepts/overview), see [docs/submit-clouddq-as-dataproc-job.md](docs/submit-clouddq-as-dataproc-job.md).


## Troubleshooting

### 1. Cannot find shared library dependencies on system
If running `make build` fails due to missing shared library dependencies (e.g. `_ctypes` or `libssl`), try the below steps to install them, then clear the bazel cache with `make clean` and retry.

#### Mac OS X:

If you haven't done so, install Xcode Command Line Tools (`xcode-select --install`) and Homebrew. Then:

```bash
# optional, but recommended:
brew install openssl readline sqlite3 xz zlib
```
#### Ubuntu/Debian/Mint:

```bash
sudo apt-get update; sudo apt-get install --no-install-recommends make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

### 2. Wrong `glibc` version
If you encounter the following error when running the executable Python zip on a different machine to the one you used to build your zip artifact:
machine:
```
/lib/x86_64-linux-gnu/libm.so.6: version `GLIBC_2.xx' not found
```
This suggests that the `glibc` version on your target machine is incompatible with the version on your build machine. Resolve this by rebuilding the zip on machine with identical `glibc` version (usually this means the same OS version) as on your target machine, or vice versa.

## License

CloudDQ is licensed under the Apache License version 2.0. This is not an official Google product.
