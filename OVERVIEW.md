# Introduction and Overview

## High-Level: What does it do and why do I need it?

TODO

## Concepts

* Andrew: Reuse and update "Introductions" section from [top level README](readme.md)


### Built-in Rule Types

The CloudDQ framework defines 5 basic types of rules:

* NOT_NULL
* NOT_BLANK
* REGEX
* CUSTOM_SQL_EXPRESSION
* CUSTOM_SQL_STATEMENT

The following table describes the details of these rules and how to use them.

#### Rule Type `NOT_NULL`

Violated if any row in the given column contains null.

The rule is applied to a column in a table (an “entity”)  through a rule binding.

```
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

Complete example TODO


#### Rule Type `NOT_BLANK`

Violated if any row in the given column contains null.

The rule is applied to a column in a table (an “entity”)  through a rule binding.
 
```
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

Complete example TODO


#### Rule Type `REGEX`

For `STRING` columns. This rule type allows the specification of a Python regular expression to define allowed patterns.

The example shows a check for a valid email address.

The rule is applied to a column in a table through a rule binding.

```
rules:
  VALID_EMAIL:
    rule_type: REGEX
    dimension: conformance
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

Complete example TODO

#### Rule Type `CUSTOM_SQL_EXPRESSION`

This rule type allows the specification of a condition using SQL (as in a WHERE clause). The validation fails when the condition is violated.

The example condition performs a SELECT on a reference table to compare to a list of known currencies.

The SQL condition references a parameter $column, which is set through the rule binding.

```
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

Complete example TODO


#### Rule Type `CUSTOM_SQL_STATEMENT`

The “custom SQL statement” allows you to specify a complete SQL statement. Any rows selected by the statement are considered violations of the rule.

The SQL statement references the source data as “data”. The dataset is defined through the entity_id field in the rule binding.

The SQL statement can also be parametrized using custom SQL arguments as shown in the example. The example provides a way to check the range of values in a column. 

This rule type is intended for set-level validation: To check properties of a set, such as the total row count, or the standard deviation of a column. Therefore, while we reported the number of rows returned as the number of failures, the reporting varies from the other rule types.

```
rules:
  VALUE_RANGE:
    rule_type: CUSTOM_SQL_STATEMENT
    custom_sql_arguments:
      - lower
      - upper
    params:
      custom_sql_statement: |-
        select * from data
        where $column < $lower or $column > $upper

rule_bindings:
  LOCATION_LAT:
    entity_id: TEST_DATA
    column_id: LATIITUDE
    row_filter_id: NONE
    rule_ids:
      - VALUE_RANGE:
          lower: 30.1859265
          upper: 30.4188298
```

TODO Complete example


### CloudDQ Execution

TODO: Explains the mechanics of how rule_bindings are ultimately applied to the data (info about rule_bindings translated to sql and pushed down to bq etc.)

From original README:

On each run, CloudDQ converts each `rule_binding` into a SQL script, create a corresponding [view](https://cloud.google.com/bigquery/docs/views) in BigQuery, and aggregate the validation results summary per `rule_binding` and `rule` for each CloudDQ run into a DQ Summary Statistics table in a BigQuery table specified in the CLI argument `--target_bigquery_summary_table`.



### Consuming CloudDQ Outputs

TODO: Explains how CloudDQ outputs are captured and can be accessed.

From original README:

You can then use any dashboarding solution such as Data Studio or Looker to visualize the DQ Summary Statistics table, or use the DQ Summary Statistics table for monitoring and alerting purposes.

