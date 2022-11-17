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
    entity_uri: bigquery://projects/<project-id>/datasets/<dataset_id>/tables/<table_id> # replace variables in <angle brackets> with your own configs
    column_id: VALUE
    row_filter_id: DATA_TYPE_EMAIL
    reference_columns_id: CONTACT_DETAILS_REFERENCE_COLUMNS
    incremental_time_filter_column_id: TS
    rule_ids:
      - NOT_NULL_SIMPLE
      - REGEX_VALID_EMAIL
      - CUSTOM_SQL_LENGTH_LE_30
    metadata:
      team: team-2

  T3_DQ_1_EMAIL_DUPLICATE:
    entity_uri: dataplex://projects/<project-id>/locations/<region-id>/lakes/<lake-id>/zones/<zone-id>/entities/<entity-id> # replace variables in <angle brackets> with your own configs
    column_id: VALUE
    row_filter_id: DATA_TYPE_EMAIL
    reference_columns_id: INCLUDE_ALL_REFERENCE_COLUMNS
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

`entity_id` refers to the table being validated as defined in a separate `entities` configuration node. `entity_uri` is the URI path in a remote metadata registry that references the table being validated. You can only define one of `entity_id` or `entity_uri` in a single `rule_binding`. If you only use `entity_uri` in your `rule_binding` configs, there is no need to define the `entities` configuration node separately.  This is because CloudDQ will fetch metadata about the `entity_uri` directly from the remote metadata registry instead of relying on manually defined `entities` configurations.  **Where possible, we recommend using the newer `entity_uri` instead of manually specifying column schemas in the `entity` configs.**

`column_id` refers to the column in the table to be validated, `row_filter_id` refers to a filter condition to select rows in-scope for validation, and `rule_ids` refers to the list of data quality validation rules to apply on the selected column and rows.

`reference_columns_id` is an optional parameter and refers to the columns names (not `column_id`) in the entity that should be included in the failed records query. The failed records query is a SQL query that is included in the [DQ output summary table](OVERVIEW.md#consuming-clouddq-outputs) containing the validation summary results. The SQL query can be executed to retrieve the records that failed the given `rule_id` validation in a `rule_binding`. `reference_columns_id` can be set to a unique identifier of a `reference_columns` node type documented [here](REFERENCE.md#reference-columns). The field `include_reference_columns` columns will have no effect for CUSTOM_SQL_STATEMENT rule types. This is because `include_reference_columns` may only refers to column names that exists in the referenced entity in the `rule_binding`, while the content of the CUSTOM_SQL_STATEMENT rule may include transformed columns such as `median(<column_name>)` or columns that do not exist in the referenced entity. 

`incremental_time_filter_column_id` is an optional parameter for incremental validation of an append-only table with a monotonically increasing timestamp column. If `incremental_time_filter_column_id` is set to a monotonically-increasing timestamp column in an append-only table, `CloudDQ` on each run will only validate rows where the timestamp specified in `incremental_time_filter_column_id` is higher than the timestamp of the last run. This allows CloudDQ to perform incremental validation without scanning the entire table everytime. If `incremental_time_filter_column_id` is not set, `CloudDQ` will validate all rows matching the `row_filter_id` on each run.

`metadata` is an optional parameter for including relevant information for each `rule_binding` as key-value pairs in the resulting output table. Under the `metadata` node, you can add any custom key-value pairs. These are propagated to the [DQ output summary table](OVERVIEW.md#consuming-clouddq-outputs) in JSON format, which allows filtering, or drill-down, in dashboards that show the validation output. This can be useful, for example, to capture the team that is responsible for a given rule binding.


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

For both `CUSTOM_SQL_EXPR` and `CUSTOM_SQL_STATEMENT`, the templated parameter `$column` in the `custom_sql_expr` and `custom_sql_statement` blocks will be substituted with the `column_id` specified in each `rule_binding`. `CUSTOM_SQL_STATEMENT` additionally supports custom parameterized variables in the `custom_sql_arguments` block. These parameters can be bound separately in each `rule_binding`, allowing `CUSTOM_SQL_STATEMENT` to be defined once to be reused across multiple `rule_binding`s with different input arguments.

The SQL code block in `CUSTOM_SQL_STATEMENT` rules must include the string `from data` in the `custom_sql_statement` block, i.e. it must validate data returned by a `data` common table expression (CTE). The common table expression `data` contains rows returned once all `row_filters` and incremental validation logic have been applied.


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

#### Reference Columns

**Reference Columns**: Defines the columns to be included in the failed records SQL query included in the output BigQuery summary table.
For the given `entity_id` in the `rule_binding`, the column names (not `column_id`) referenced in the `include_reference_columns` field will be included in the resulting output of the failed records SQL query. This allows you to include relevant unique identifier columns or context for the failed records for diagnostic puposes. 

The `include_reference_columns` field can specify `"*"` to include all columns from the entity referenced in the `rule_binding` in the resulting failed records query. If `include_reference_columns` contains `"*"`, it cannot include any other column names.

The field `include_reference_columns` columns will have no effect for CUSTOM_SQL_STATEMENT rule types. This is because `include_reference_columns` may only refers to column names that exists in the referenced entity in the `rule_binding`, while the content of the CUSTOM_SQL_STATEMENT rule may include transformed columns such as `median(<column_name>)` or columns that do not exist in the referenced entity.

```yaml
reference_columns:

  CONTACT_DETAILS_REFERENCE_COLUMNS:
    include_reference_columns:
      - row_id
      - contact_type
      - value

  INCLUDE_ALL_REFERENCE_COLUMNS:
    include_reference_columns:
      - "*"
```

#### Entities

**Entities**: Captures static metadata for entities that are referenced in rule bindings.

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
Entities configurations contain information of the table to be validated, including its source database, connnection settings, and column configurations.

`entities` can be referenced in a `rule_binding` by specifying its unique identifier in the `rule_binding` field `entity_id`. *If you do not use `entity_id` in any of your `rule_binding`, there is no need to define the `entities` configuration node separately.*

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
 TRANSACTIONS_UNIQUE_1:
   entity_uri: bigquery://projects/<project-id>/datasets/<dataset_id>/tables/<table_id> # replace variables in <angle brackets> with your own configs
   column_id: id
   row_filter_id: NONE
   rule_ids:
     - NO_DUPLICATES_IN_COLUMN_GROUPS:
         column_names: "id"
         
 TRANSACTIONS_UNIQUE_2:
   entity_uri: dataplex://projects/<project-id>/locations/<region-id>/lakes/<lake-id>/zones/<zone-id>/entities/<entity-id> # replace variables in <angle brackets> with your own configs
   column_id: id
   row_filter_id: NONE
   rule_ids:
     - NO_DUPLICATES_IN_COLUMN_GROUPS:
         column_names: "id"
```
Instead of manually specifying the `entities` configuration to be referenced in the `entity_id` field of each `Rule Binding`, you can specify an `entity_uri` containing the lookup path to a remote metadata registry referencing the table entity to be validated. This omit the need to define the `entities` configurations and the associated schemas directly.

The remote metadata registry can be specified using the URI scheme `dataplex://` to use the Dataplex Metadata API or `bigquery://` to use the BigQuery Tables API. The arguments following the URI scheme are key value pairs for looking up the table entity in the respective metadata registry. For example, `bigquery://projects/<project-id>/datasets/<dataset_id>/tables/<table_id>` for referencing BigQuery tables, or `dataplex://projects/<project-id>/locations/<region-id>/lakes/<lake-id>/zones/<zone-id>/entities/<entity-id>` for referencing a Dataplex entity, which may refers to a BigQuery table or a GCS bucket.

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
