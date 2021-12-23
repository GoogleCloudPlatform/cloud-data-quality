# Reference Guide

## Configuration Specification

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
Each `rule_binding` must define one of the fields: 

- `entity_id` or `entity_uri` and 
- all of the fields `column_id`, `row_filter_id`, and `rule_ids`.

`entity_id` refers to the table being validated, `entity_uri` is the URI path in a remote metadata registry that references the table being validated, `column_id` refers to the column in the table to be validated, `row_filter_id` refers to a filter condition to select rows in-scope for validation, and `rule_ids` refers to the list of data quality validation rules to apply on the selected column and rows.

If `incremental_time_filter_column_id` is set to a monotonically-increasing timestamp column in an append-only table, `CloudDQ` on each run will only validate rows where the timestamp specified in `incremental_time_filter_column_id` is higher than the timestamp of the last run. This allows CloudDQ to perform incremental validation without scanning the entire table everytime. If `incremental_time_filter_column_id` is not set, `CloudDQ` will validate all rows matching the `row_filter_id` on each run.

Under the `metadata` config, you may add any key-value pairs that will be added as a JSON on each [DQ summary output record](OVERVIEW.md#consuming-clouddq-outputs). For example, this can be the team responsible for a `rule_binding`, the table type (raw, curated, reference). The JSON allows custom aggregations and drilldowns over the summary data.

> TODO Rephrase the above

#### Rule Dimensions

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

#### Rules

**Rules**: Defines reusable logic for data quality validation.

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

`CUSTOM_SQL_EXPR` will apply the SQL expression logic defined in `custom_sql_expr` to every record and flag any row where `custom_sql_expr` evaluated to False as a failure. `CUSTOM_SQL_EXPR` rules are meant for record-level validation and will report success/failure counts at the record level. 

`CUSTOM_SQL_STATEMENT` is meant for set-level validation and instead accepts a complete SQL statement in the field `custom_sql_statement`. If the SQL statement returns 0 rows, `CloudDQ` considers the rule a success. If the SQL statement returns any row, `CloudDQ` will report the rule as a failure and include the returned row counts in the validation summary.

> TODO Note Andrew: Suggest we bring up more clearly the distinction between row- and set-level validations, following with how custom_sql_expr/statement (and built-in rules) support them

For both `CUSTOM_SQL_EXPR` and `CUSTOM_SQL_STATEMENT`, the templated parameter `$column` in the `custom_sql_expr` and `custom_sql_statement` blocks will be substituted with the `column_id` specified in each `rule_binding`. `CUSTOM_SQL_STATEMENT` additionally supports custom parameterized variables in the `custom_sql_arguments` block. These parameters can be bound separately in each `rule_binding`, allowing `CUSTOM_SQL_STATEMENT` to be defined once to be reused across multiple `rule_binding`s with different input arguments.

The SQL code block in `CUSTOM_SQL_STATEMENT` rules must include the string `from data` in the `custom_sql_statement` block, i.e. it must validate data returned by a `data` common table expression (CTE). The common table expression `data` contains rows returned once all `row_filters` and incremental validation logic have been applied.

> TODO ?? the user can choose to read from the underlying entity (for ex. when they want to ignore incremental logic. We need to explain various usage scenarios and best practices here

#### Filters

**Filters**: Defines a filtering condition for data extracted from the respective entity to generate a sub-set of data for validation

The content of the `filter_sql_expr` field will be inserted into a SQL `WHERE` clause for filtering your data to the rows for validation.

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

#### Entities

**Entities**: Captures static metadata for entities that are referenced from rule bindings.

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

#### Metadata Registry Defaults

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



## Library Reference