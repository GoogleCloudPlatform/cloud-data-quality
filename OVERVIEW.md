# Introduction and Overview

## High-Level: What does it do and why do I need it?

`CloudDQ` is a solution component of Data Quality management that supports ongoing Data Quality evaluation of evolving data assets within live data environments, driven by a declarative configuration:

* “ongoing evaluation” –  repeated, frequent (in application to the same data assets) – as opposed to one-off
* “evolving data assets” – data assets that mutate and grow over time – as opposed to constant/static data assets
* “live data environments” – environments supporting production data operations – as opposed to migration or dev/test environments
* “declarative configuration” – specifies intended goals (what) of DQ evaluation from an end user perspective – as opposed to imperative (how) approach

The declarative configuration specification is described in [Reference Guide](REFERENCE.md).

## Concepts


### Built-in Rule Types

The CloudDQ framework defines 5 basic types of rules:

* `NOT_NULL`
* `NOT_BLANK`
* `REGEX`
* `CUSTOM_SQL_EXPRESSION`
* `CUSTOM_SQL_STATEMENT`

Of these rules, the first four support row-level validation and the last one supports set-level validation. We will begin by explaining this difference before proceeding to explain each rule type in detail.

#### Row-level Versus Set-level Validation

Row-level validation checks attributes of individual rows or elements of the data: For example whether the value is in a given range, or whether all fields are internally consistent. There are also validation checks that cannot be performed by just inspecting one row or element. We call this "set-level validation". Possibly the simplest example of set-level validation is a check on the total row count.

An important difference between row-level and set-level validation is that in the case of set-level validation, in general, one cannot say which rows cause a check to fail. Also, in some cases it's not possible to say how many rows fail the check. With row-level validation on the other hand, we can connect each validation error to a specific row. This difference is reflected in the `CloudDQ` output. For row-level validation, `CloudDQ` reports the number of rows validated, how many were successful, how many produced errors and how many were null. For set-level checks, `CloudDQ` only provides how many rows the check was run on, how many errors were generated, plus an overall success or failure flag.

The number of errors from set-level validation may or may not have a relation to a number of rows, depending on how the check is coded. For example, for a check on uniqueness, the number of errors may equal the number of duplicated rows, the number of elements that are duplicated, or something else entirely.

Of the built-in rule types, `CUSTOM_SQL_STATEMENT` is intended for set-level validation. That means that any check implemented using this rule type will produce output that can be expected from set-level validation.


#### Rule Type `NOT_NULL`

Violated if any row in the given column contains null.

```yaml
rules:
  NOT_NULL_SIMPLE:
    rule_type: NOT_NULL

rule_bindings:
  NOT_NULL:
    entity_id: TEST_DATA
    column_id: UNIQUE_KEY
    row_filter_id: NONE
    rule_ids:
      - NOT_NULL_SIMPLE
```

Complete example: [Rule](configs/rules/base-rules.yml) and [rule binding](configs/rule_bindings/team-1-bindings.yml)


#### Rule Type `NOT_BLANK`

Violated if any row in the given column contains an empty string.
 
```yaml
rules:
  NOT_BLANK_SIMPLE:
    rule_type: NOT_BLANK

rule_bindings:
  NOT_BLANK:
    entity_id: TEST_DATA
    column_id: UNIQUE_KEY
    row_filter_id: NONE
    rule_ids:
      - NOT_BLANK_SIMPLE
```

Complete example: [Rule](configs/rules/base-rules.yml) and [rule binding](configs/rule_bindings/team-2-bindings.yml)


#### Rule Type `REGEX`

For `STRING` columns. This rule type allows the specification of a Python regular expression to define allowed patterns.

The example shows a check for a valid email address.

```yaml
rules:
  VALID_EMAIL:
    rule_type: REGEX
    params:
      pattern: |-
        ^[^@]+[@]{1}[^@]+$

rule_bindings:
  VALID_EMAIL:
    entity_id: TEST_DATA
    column_id: EMAIL
    row_filter_id: NONE
    rule_ids:
      - VALID_EMAIL
```

Complete example: [Rule](configs/rules/base-rules.yml) and [rule binding](configs/rule_bindings/team-2-bindings.yml)

#### Rule Type `CUSTOM_SQL_EXPRESSION`

This rule type allows the specification of an SQL expression returning a boolean. The validation fails when the expression returns `FALSE`.

This rule, contrary to `CUSTOM_SQL_STATEMENT` supports row-level validation (see preceding section on the differences between row-level and
set-level validation). This implies it has access to the row-level context of the table defined as "entity", meaning that all columns of the
same row can be accessed.

The below example of this rule performs a SELECT on a reference table to compare to a list of known currencies.

The SQL expression can be parametrized using `custom_sql_parameters` (see the `CUSTOM_SQL_LENGTH` [example rule](configs/rules/base-rules.yml) and [how it's used](configs/rule_bindings/team-2-rule-bindings.yml)). The SQL expression can also use an implicit parameter `$column`, to identify the column that the condition should be applied to. The value of the parameter `$column` refers to a column in the referred entity. In the example below, the rule is applied to column `CURRENCY` in the `TEST_DATA` entity, which in turn refers to a column called `curr` in a table in BigQuery.

```yaml
entities:
  TEST_DATA:
    source_database: BIGQUERY
    table_name: <table_id>
    dataset_name: <dataset_id>
    project_name: <project_id>
    columns:
      CURRENCY:
        name: curr
        data_type: STRING

rules:
  CORRECT_CURRENCY_CODE:
    rule_type: CUSTOM_SQL_EXPR
    dimension: conformance
    params:
      custom_sql_expr: |-
        $column in (select distinct currency_code from `<reference_dataset-id>.currency_codes`)

rule_bindings:
  LOCATION_LAT:
    entity_id: TEST_DATA
    column_id: CURRENCY
    row_filter_id: NONE
    rule_ids:
      - CORRECT_CURRENCY_CODE
```

Example rules can be found in [base-rules.yml](configs/rules/base-rules.yml).


#### Rule Type `CUSTOM_SQL_STATEMENT`

The “custom SQL statement” allows you to specify set-level validation through a custom SQL statement returning a dataset (i.e. `SELECT ... FROM ...`). The statement should indicate a violation of the rule through returning a non-empty set of records.

The SQL statement references the source data as “data”. The dataset is defined through the entity_id field in the rule binding.

The SQL statement can also be parametrized using an implicit parameter `column` and custom SQL arguments, as shown in the example below. The rule binding sets the value of the `column` parameter through the `column_id` field and the custom parameters are set explicitly by listing them on each rule reference.

This rule type is intended for set-level validation. See the paragraph on the row-level and set-level validation at the start of this section for more information.

```yaml
rules:
  ROW_COUNT:
    rule_type: CUSTOM_SQL_STATEMENT
    custom_sql_arguments:
      n_max
    params:
      custom_sql_statement: |-
        select 
          count(
              case when $column is null then 0
              else $column
              end) as n
        from data
        where n > n_max

rule_bindings:
  ROW_COUNT:
    entity_id: TEST_DATA
    column_id: TX_ID
    row_filter_id: NONE
    rule_ids:
      - ROW_COUNT:
          n_max: 1000
```

Complete example: [Rule](configs/rules/complex-rules.yml) and [rule binding](configs/rule_bindings/team-3-bindings.yml)


### CloudDQ Execution

On each run, CloudDQ converts each `rule_binding` into a SQL script, which creates a corresponding [view](https://cloud.google.com/bigquery/docs/views) in BigQuery, and aggregates the validation results summary per `rule_binding` and `rule` for each CloudDQ run into a DQ Summary Statistics table in a BigQuery table specified in the CLI argument `--target_bigquery_summary_table`.



### Consuming CloudDQ Outputs

`CloudDQ` will generate a summary table called `dq_summary`, and a view for each rule binding. You can visualize the results using any dashboarding solution such as Data Studio or Looker to visualize the DQ Summary Statistics table, or use the DQ Summary Statistics table for monitoring and alerting purposes.

`CloudDQ` reports validation summary statistics for each `rule_binding` and `rule` by appending to the target BigQuery table specified in the CLI argument `--target_bigquery_summary_table`. Record-level validation statistics are captured the in columns `success_count`, `success_percentage`, `failed_count`, `failed_percentage`, `null_count`, `null_percentage`. The rule type `NOT_NULL` reports the count of `NULL`s present in the input `column-id` in the columns `failed_count`, `failed_percentage`, and always set the columns `null_count` and `null_percentage` to `NULL`. `CUSTOM_SQL_STATEMENT` rules do not report record-level validation statistics and therefore will set the content of the columns `success_count`, `success_percentage`, `failed_count`, `failed_percentage`, `null_count`, `null_percentage` to `NULL`.

Set-level validation results for `CUSTOM_SQL_STATEMENT` rules are captured in the columns `complex_rule_validation_errors_count` and `complex_rule_validation_success_flag`. `complex_rule_validation_errors_count` contains the count of rows returned by the `custom_sql_statement` block. `complex_rule_validation_success_flag` is set to `TRUE` if `complex_rule_validation_errors_count` is equals to 0, `FALSE` if `complex_rule_validation_errors_count` is larger than 0, and `NULL` for all other rule types that are not `CUSTOM_SQL_STATEMENT`.

The summary table stores the results of the data quality validations, with the
output summary for each combination of rule binding and rule per validation run.
This output is structured in the summary table as follows:

#### Summary Table Description

The table below lists the columns in their `dq_summary` table, that is the output of a CloudDQ validation run.

<table>
  <thead>
    <tr>
      <th>Column name</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>execution_ts</code></td>
      <td>(timestamp) Timestamp of when the validation query was executed.</td>
    </tr>
    <tr>
      <td><code>rule_binding_id</code></td>
      <td>(string) ID of the rule binding for which validation results are
        reported.</td>
    </tr>
    <tr>
      <td><code>rule_id</code></td>
      <td>(string) ID of the rule under the rule binding for which validation
        results are reported.</td>
    </tr>
    <tr>
      <td><code>dimension</code></td>
      <td>(string) Data Quality dimension of the rule_id. This value can only be 
       one of the values specified in the <code>rule_dimensions</code> YAML node.</td>
    </tr>
    <tr>
      <td><code>table_id</code></td>
      <td>(string) ID of the table for which validation results are reported.</td>
    </tr>
    <tr>
      <td><code>column_id</code></td>
      <td>(string) ID of the column for which validation results are reported.</td>
    </tr>
    <tr>
      <td><code>last_modified</code></td>
      <td>(timestamp) The last modified timestamp of the <code>table_id</code>
        being validated.</td>
    </tr>
    <tr>
      <td><code>metadata_json_string</code></td>
      <td>(string) Key-value pairs of the metadata parameter content specified
       under the rule binding or during the data quality run.</td>
    </tr>
    <tr>
      <td><code>configs_hashsum</code></td>
      <td>(string) The hash sum of the JSON document containing the rule binding
        and all its associated rules, rule bindings, row filters, and entities
        configurations. This allows tracking when the content of a
        <code>rule_binding</code> ID or one of its referenced configurations has
        changed.</td>
    </tr>
    <tr>
      <td><code>dq_run_id</code></td>
      <td>(string) Unique ID of the record.</td>
    </tr>
    <tr>
      <td><code>invocation_id</code></td>
      <td>(string) ID of the data quality run. All data quality summary records
        generated within the same data quality execution instance share the same
        <code>invocation_id</code>.</td>
    </tr>
    <tr>
      <td><code>progress_watermark</code></td>
      <td>(boolean) Determines whether this particular record will be considered
        by the data quality check in determining high watermark for incremental
        validation. If FALSE, the respective record will be ignored when
        establishing the high watermark value. This is useful when executing
        test data quality validations which should not advance high watermark.
        <code>CloudDQ</code> populates this field with <code>TRUE</code> by default, but
        this can be overridden if the argument <code>--progress_watermark</code>
        has a value of <code>FALSE</code>.
      </td>
    </tr>
        <tr>
      <td><code>rows_validated</code></td>
      <td>(integer) Total number of records validated after applying
        <code>row_filters</code> and any high-watermark filters on the
        <code>incremental_time_filter_column_id</code> column if specified.</td>
    </tr>
    <tr>
      <td><code>complex_rule_validation_errors_count</code></td>
      <td>(float) Number of rows returned by a <code>CUSTOM_SQL_STATEMENT</code>
        rule.</td>
    </tr>
    <tr>
      <td><code>complex_rule_validation_success_flag</code></td>
      <td>(boolean) Success status of <code>CUSTOM_SQL_STATEMENT</code> rules.
    </td>
    </tr>
    <tr>
      <td><code>success_count</code></td>
      <td>(integer) Total number of records that passed validation. This field
        is set to <code>NULL</code> for <code>CUSTOM_SQL_STATEMENT</code> rules.
      </td>
    </tr>
    <tr>
      <td><code>success_percentage</code></td>
      <td>(float) Percentage of the number of records that passed validation
        within the total number of records validated. This field is set to
        <code>NULL</code> for <code>CUSTOM_SQL_STATEMENT</code> rules.</td>
    </tr>
    <tr>
      <td><code>failed_count</code></td>
      <td>(integer) Total number of records that failed validation. This field
        is set to <code>NULL</code> for <code>CUSTOM_SQL_STATEMENT</code> rules.
      </td>
    </tr>
        <tr>
      <td><code>failed_percentage</code></td>
      <td>(float) Percentage of the number of records that failed validation
        within the total number of records validated. This field is set to
        <code>NULL</code> for <code>CUSTOM_SQL_STATEMENT</code> rules.</td>
    </tr>
    <tr>
      <td><code>null_count</code></td>
      <td>(integer) Total number of records that returned null during validation.
         This field is set to <code>NULL</code> for <code>NOT_NULL</code> and
         <code>CUSTOM_SQL_STATEMENT</code> rules.</td>
    </tr>
    <tr>
      <td><code>null_percentage</code></td>
      <td>(float) Percentage of the number of records that returned null during
        validation within the total number of records validated. This field is
        set to <code>NULL</code> for <code>NOT_NULL</code> and
        <code>CUSTOM_SQL_STATEMENT</code> rules.</td>
    </tr>
  </tbody>
</table>
