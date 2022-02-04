# Advanced Rules <!-- omit in toc --> 
## Example Use Cases <!-- omit in toc --> 
As an extension to existing examples and quickstart overview of CloudDQ rule framework, some more advanced rules have been prepared that address common data quality scenarios. The list is not full (will be kept updated) but contains most popular dimensions like `timeliness`, `correctness`, `integrity`, `conformity`, `completness` just to name a few.

- [Timeliness](#timeliness)
  - [Alerting for failed / delayed data ingestion](#alerting-for-failed--delayed-data-ingestion)
  - [Comparing data volumes / record counts between two time periods](#comparing-data-volumes--record-counts-between-two-time-periods)
- [Correctness](#correctness)
  - [Define complex rule with record columns](#define-complex-rule-with-record-columns)
- [Integrity](#integrity)
  - [Validating Integrity (against reference data)](#validating-integrity-against-reference-data)
  - [Validating Integrity (against set returned by subquery)](#validating-integrity-against-set-returned-by-subquery)
- [Conformity](#conformity)
  - [Validating Conformity](#validating-conformity)
- [Completness](#completness)
  - [Validating Completeness](#validating-completeness)
- [Uniqueness](#uniqueness)
  - [Validating Uniqueness](#validating-uniqueness)
- [Accuracy](#accuracy)
  - [Validating Accuracy](#validating-accuracy)
  
### Timeliness
#### Alerting for failed / delayed data ingestion
This rule identifies if there was no data ingested in a period of time of a specified duration directly preceding the current moment. There are 3 levels to choose from: `month` (stored in 'yyyymm' format), `day` and `timestamp`.

Input parameters:
- ingestion_date_[month|day|timestamp] - column that store date/timestamp dimension
- elapsed_time_[months|days|hours] - lookback window of X number of months/days/hours

Example config:
```yaml
rules:
  NO_DELAYED_INGESTION_DAY_LEVEL:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: timeliness
    params:
      custom_sql_arguments:
        - ingestion_date_day
        - elapsed_time_days
      custom_sql_statement: |-
        select * from
        (select count(*) as n
          from data a
          where $ingestion_date_day >= date_sub(current_date(), interval $elapsed_time_days day) 
        )
        where n = 0
  NO_DELAYED_INGESTION_MONTH_LEVEL:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: timeliness
    params:
      custom_sql_arguments:
        - ingestion_date_month
        - elapsed_time_months
      custom_sql_statement: |-
        select * from
        (select count(*) as n
          from data a
          where parse_date('%Y%m',  cast($ingestion_date_month as string)) >= date_sub(date_trunc(current_date(), month), interval $elapsed_time_months month) 
        )
        where n = 0
  NO_DELAYED_INGESTION_TIMESTAMP_LEVEL:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: timeliness
    params:
      custom_sql_arguments:
        - ingestion_timestamp
        - elapsed_time_hours
      custom_sql_statement: |-
        select * from
        (select count(*) as n
          from data a
          where $ingestion_timestamp >= timestamp_sub(current_timestamp(), interval $elapsed_time_hours hour) 
        )
        where n = 0
rule_bindings:
  NO_DELAYED_INGESTION_DAY_LEVEL:
    entity_id: INGESTION_TABLE_DAY_LEVEL
    column_id: INVOICE_ID
    row_filter_id: NONE
    rule_ids:
      - NO_DELAYED_INGESTION_DAY_LEVEL:
          ingestion_date_day: date_of_day
          elapsed_time_days: 11
  NO_DELAYED_INGESTION_MONTH_LEVEL:
    entity_id: INGESTION_TABLE_MONTH_LEVEL
    column_id: CLIENT_CD
    row_filter_id: NONE
    rule_ids:
      - NO_DELAYED_INGESTION_MONTH_LEVEL:
          ingestion_date_month: month_id
          elapsed_time_months: 1
  NO_DELAYED_INGESTION_TIMESTAMP_LEVEL:
    entity_id: INGESTION_TABLE_TIMESTAMP_LEVEL
    column_id: SALES_MANAGER_ID
    row_filter_id: NONE
    rule_ids:
      - NO_DELAYED_INGESTION_TIMESTAMP_LEVEL:
          ingestion_timestamp: dana_ingestion_timestamp
          elapsed_time_hours: 1200
```
Full example: [rule](/docs/examples/advanced_rules/timeliness_delayed_ingestion.yaml#86) and [rule binding](/docs/examples/advanced_rules/timeliness_delayed_ingestion.yaml#133)

#### Comparing data volumes / record counts between two time periods
This rule compares data generated for a specific time interval against another interval.

Context:
Scenarios requiring support:
- (A) Is the amount of data generated for a specific time period (for example day) for a specific reference data (shop id for example) bigger than a threshold?
- (B) Is the amount of data generated for a specific time period (for example day) for a specific reference data (shop id for example) roughly equal to average amount of data for the same time period?
- (C) Is the amount of data generated for a specific time period (for example day) for a specific reference data (shop id for example) roughly equal to the amount of data generated for the same time period same time ago (last week for example)?

The design assumes time intervals are defined as DAY, WEEK, MONTH, YEAR with interval boundaries defined by calendar boundaries of these intervals. 


Input parameters:

General:
- ts_column - timestamp column that belongs to the validated table and defines time intervals
- as_at_ts - timestamp designating the intended time interval to be checked against
- interval - time interval (ie DAY, WEEK, MONTH, YEAR)

(A) scenario specific parameters:
- threshold - fixed threshold value to compare against

(B) sceneario specific parameters:
- avg_from_ts - timestamp of a period from which average counts will be calculated (to prevent complete table rescan)
- deviation_threshold_pct - percentage of deviation allowed when comparing actual count against the reference

(C) scenario specific parameters:
- N_periods_back - integer count of period durations that will be subtracted from $as_at_ts to define a previous period for comparison
- deviation_threshold_pct - percentage of deviation allowed when comparing actual count against the reference

Example config:
```yaml
rules:
  NO_DIFFERENT_VOLUMES_PER_SPECIFIC_PERIOD:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: timeliness
    params:
      custom_sql_arguments:
        - ts_column
        - as_at_ts
        - interval
        - threshold
      custom_sql_statement: |-
        select *
        from
          (select count(*) as n 
          from data
          where date_trunc(cast($ts_column as date), $interval) = date_trunc(cast(parse_timestamp("%F %T %Z", "$as_at_ts") as date), $interval)
          )
        where  n <= $threshold
  NO_DIFFERENT_VOLUMES_PER_AVERAGE_PERIOD:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: timeliness
    params:
      custom_sql_arguments:
        - ts_column
        - as_at_ts
        - interval
        - avg_from_ts
        - deviation_threshold_pct
      custom_sql_statement: |-
        select *
        from
          (select count(*) as n
          from data
          where date_trunc(cast($ts_column as date), $interval) = date_trunc(cast(parse_timestamp("%F %T %Z", "$as_at_ts") as date), $interval)
          )
        where ifnull(safe_divide(n, (select avg(count_by_interval)
          from (
            select count(*) as count_by_interval
            from data
            where
            date_trunc(cast($ts_column as date), $interval) >= date_trunc(cast(parse_timestamp("%F %T %Z", "$avg_from_ts") as date), $interval)
            group by date_trunc(cast($ts_column as date), $interval)
          )
        )), cast('inf' as float64)) not between (1 - ($deviation_threshold_pct / 100)) and (1 + ($deviation_threshold_pct / 100))
  NO_DIFFERENT_VOLUMES_PER_LAST_PERIOD:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: timeliness
    params:
      custom_sql_arguments:
        - ts_column
        - as_at_ts
        - interval
        - N_periods_back
        - deviation_threshold_pct
      custom_sql_statement: |-
        select *
        from
          (select count(*) as n
          from data
          where date_trunc(cast($ts_column as date), $interval) = date_trunc(cast(parse_timestamp("%F %T %Z", "$as_at_ts") as date), $interval)
          )
        where ifnull(safe_divide(n, (select count(*) as count_by_interval
          from data
          where
          date_trunc(cast($ts_column as date), $interval) = date_sub(cast(timestamp_trunc("$as_at_ts", $interval) as date), interval $N_periods_back $interval)
        )), cast('inf' as float64)) not between (1 - ($deviation_threshold_pct / 100)) and (1 + ($deviation_threshold_pct / 100))

rule_bindings:
  NO_DIFFERENT_VOLUMES_PER_SPECIFIC_PERIOD_DAY:
    entity_id: DIFFERENT_VOLUMES_PER_PERIOD
    column_id: DANA_INGESTION_TIMESTAMP
    row_filter_id: NONE
    rule_ids:
      - NO_DIFFERENT_VOLUMES_PER_SPECIFIC_PERIOD:
          ts_column: dana_ingestion_timestamp
          as_at_ts: "2021-12-23 00:00:00 UTC"
          interval: day
          threshold: 100
    metadata:
      brand: one
  NO_DIFFERENT_VOLUMES_PER_AVERAGE_PERIOD_DAY:
    entity_id: DIFFERENT_VOLUMES_PER_PERIOD
    column_id: DANA_INGESTION_TIMESTAMP
    row_filter_id: NONE
    rule_ids:
      - NO_DIFFERENT_VOLUMES_PER_AVERAGE_PERIOD:
          ts_column: dana_ingestion_timestamp
          as_at_ts: "2021-12-22 00:00:00 UTC"
          avg_from_ts: "2021-12-01 00:00:00 UTC"
          interval: day
          deviation_threshold_pct: 15
    metadata:
      brand: one
  NO_DIFFERENT_VOLUMES_PER_LAST_PERIOD_DAY:
    entity_id: DIFFERENT_VOLUMES_PER_PERIOD
    column_id: DANA_INGESTION_TIMESTAMP
    row_filter_id: NONE
    rule_ids:
      - NO_DIFFERENT_VOLUMES_PER_LAST_PERIOD:
          ts_column: dana_ingestion_timestamp
          as_at_ts: "2021-12-22 00:00:00 UTC"
          interval: day
          N_periods_back: 1
          deviation_threshold_pct: 30
```
Full example: [rule](/docs/examples/advanced_rules/timelines_volumes_per_perdiod.yaml#45) and [rule binding](/docs/examples/advanced_rules/timelines_volumes_per_perdiod.yaml#113)

### Correctness
#### Define complex rule with record columns
This rule is applied at the row level (can be run in the incremental fashion using watermark) and is checking the business condition - invoice gross total amount of sale line items. 

Note: This example uses nested fields column type in BigQuery that needs to be first unnested, before the sum function is applied.

Input parameters:
- error_margin - a float value of accepted error margin

Example config:
```yaml
rules:
  NO_COMPLEX_RULES_MISMATCH:
    rule_type: CUSTOM_SQL_EXPR
    dimension: correctness
    params:
      custom_sql_arguments:
        - error_margin
      custom_sql_expr: |-
        (select
            sum(SaleLine.dItemTotalNetAmount) + sum(SaleLine.dVatAmount)
          from
            unnest(SaleLineList.SaleLine) as SaleLine
        ) between $column - $error_margin and $column + $error_margin

rule_bindings:
  NO_COMPLEX_RULES_MISMATCH_SHOULD_SUCCEED:
    entity_id: COMPLEX_RULES_TABLE_OK
    column_id: INVOICE_GROSS_TOTAL_AMOUNT
    row_filter_id: NONE
    rule_ids:
      - NO_COMPLEX_RULES_MISMATCH:
          error_margin: 0.03
```
Full example: [rule](/docs/examples/advanced_rules/correctness_complex_rule.yaml#57) and [rule binding](/docs/examples/advanced_rules/correctness_complex_rule.yaml#72)

### Integrity
#### Validating Integrity (against reference data)
This check validates whether values in a specific column belong to a set maintained in a reference table.

Note: This check can be implemented using both `IN` operators and `EXISTS` operators. Depending on the nature of the reference data, each approach might be more performant. Generally speaking, if the reference data set is small, then `IN` might work better than the `EXISTS` operator that does join both tables, but it's optimised to return as soon as there's a match.

Note2: Below queries assumes that reference data table is within the same project_id, if that's not the case, there should be added a new _project_id_ parameter and also appropriate permission for the CloudDQ Service Account which is used when running it.

Input parameters:
- ref_data_dataset - dataset name where the reference data table is
- ref_data_table_id - reference data table id

Example config:
```yaml
rules:
  NO_REFERENTIAL_INTEGRITY_VIOLATION_IN_OPERATOR:
    rule_type: CUSTOM_SQL_EXPR
    dimension: integrity
    params:
      custom_sql_arguments:
        - ref_data_dataset
        - ref_data_table_id
        - ref_data_column_id
      custom_sql_expr: |-
        $column in (select $ref_data_column_id from `$ref_data_dataset.$ref_data_table_id`)
  NO_REFERENTIAL_INTEGRITY_VIOLATION_EXISTS_OPERATOR:
    rule_type: CUSTOM_SQL_EXPR
    dimension: integrity
    params:
      custom_sql_arguments:
        - ref_data_dataset
        - ref_data_table_id
        - ref_data_column_id
      custom_sql_expr: |-
        exists (select 1 from `$ref_data_dataset.$ref_data_table_id` r where r.$ref_data_column_id = data.$column)

rule_bindings:
  T4_REFERENTIAL_INTEGRITY_VIOLATION_IN_OPERATOR_SHOULD_SUCCEED:
    entity_id: REFERENCE_DATA_CHECK_OK
    column_id: ART_NO
    row_filter_id: NONE
    rule_ids:
      - NO_REFERENTIAL_INTEGRITY_VIOLATION_IN_OPERATOR:
          ref_data_dataset: <my_bigquery_input_data_dataset_id>
          ref_data_table_id: reference_data
          ref_data_column_id: art_no
  T4_REFERENTIAL_INTEGRITY_VIOLATION_EXISTS_OPERATOR_SHOULD_SUCCEED:
    entity_id: REFERENCE_DATA_CHECK_OK
    column_id: ART_NO
    row_filter_id: NONE
    rule_ids:
      - NO_REFERENTIAL_INTEGRITY_VIOLATION_EXISTS_OPERATOR:
          ref_data_dataset: <my_bigquery_input_data_dataset_id>
          ref_data_table_id: reference_data
          ref_data_column_id: art_no
```
Full example: [rule](/docs/examples/advanced_rules/integrity_reference_data.yaml#57) and [rule binding](/docs/examples/advanced_rules/integrity_reference_data.yaml#78)

#### Validating Integrity (against set returned by subquery)
This check validates whether values in a specific column belong to a set returned from a subquery. It is very similar to the previous check, but the subquery might include some additional business logic or, like in this example, check the referential integrity on a set of different columns.

Note: Below queries assumes that reference data table is within the same project_id, if that's not the case, there should be added a new _project_id_ parameter and also appropriate permission for the clouddq SA which is used when running it.

Input parameters:
- ref_data_dataset - dataset name where the reference data table is
- ref_data_table_id - reference data table id

Example config:
```yaml
rules:
  NO_REFERENTIAL_INTEGRITY_VIOLATION_SUBQUERY:
    rule_type: CUSTOM_SQL_EXPR
    dimension: integrity
    params:
      custom_sql_arguments:
        - ref_data_dataset
        - ref_data_table_id
      custom_sql_expr: |-
        exists (select 1 from unnest($column.saleline) sl inner join `$ref_data_dataset.$ref_data_table_id` r on sl.litemnumber = r.subsys_item)
  NO_REFERENTIAL_INTEGRITY_VIOLATION_SUBQUERY2:
    rule_type: CUSTOM_SQL_EXPR
    dimension: integrity
    params:
      custom_sql_arguments:
        - ref_data_dataset
        - ref_data_table_id
      custom_sql_expr: |-
        exists (select 1 from unnest($column) t inner join `$ref_data_dataset.$ref_data_table_id` r on t.iq = r.id and t.type = r.type and t.qyty = r.data)

rule_bindings:
  REFERENTIAL_INTEGRITY_VIOLATION_SUBQUERY:
    entity_id: REFERENCE_DATA_CHECK_SUBQUERY_OK
    column_id: SALE_LINE_LIST
    row_filter_id: NONE
    rule_ids:
      - NO_REFERENTIAL_INTEGRITY_VIOLATION_SUBQUERY:
          ref_data_dataset: <my_bigquery_input_data_dataset_id>
          ref_data_table_id: reference_data_subquery
  REFERENTIAL_INTEGRITY_VIOLATION_SUBQUERY2:
    entity_id: REFERENCE_DATA_CHECK_SUBQUERY2_OK
    column_id: YCCOUNTIQENTIYIERS
    row_filter_id: NONE
    rule_ids:
      - NO_REFERENTIAL_INTEGRITY_VIOLATION_SUBQUERY2:
          ref_data_dataset: <my_bigquery_input_data_dataset_id>
          ref_data_table_id: reference_data_subquery2
```
Full example: [rule](/docs/examples/advanced_rules/integrity_subquery.yaml#81) and [rule binding](/docs/examples/advanced_rules/integrity_subquery.yaml#102)

### Conformity
#### Validating Conformity
This check validates whether values in a specific column belong to a predefined interval or they conform with a defined standard. In this example, they should be a valid email addresses.

Note: More info about the official email regex that has been used here, can be find on [www.regular-expressions.info](https://www.regular-expressions.info/email.html).

Example config:
```yaml
rules:
  NO_INVALID_EMAIL:
    rule_type: REGEX
    dimension: conformity
    params:
      pattern: |-
        ^[\\w\\.-]+@([\\w-]+\\.)+[\\w-]{2,}$

rule_bindings:
  CONFORMITY_CHECK:
    entity_id: CONFORMITY_CHECK_OK
    column_id: DATA
    row_filter_id: NONE
    rule_ids:
      - NO_INVALID_EMAIL
```
Full example: [rule](/docs/examples/advanced_rules/conformity_email_regex.yaml#57) and [rule binding](/docs/examples/advanced_rules/conformity_email_regex.yaml#66)

### Completness
#### Validating Completeness
This check is meant to measure the completeness of data, defined as percentage of rows (within current CloudDQ run or across the whole table) with values in a single column satisfying a certain condition, so it can be ensured that potentially missing values in a field are within an acceptable range.

Input parameters:
- threshold_pct - fixed percentage points threshold value to compare against
- condition - condition to be checked against

Example config:
```yaml
rules:
  NO_ISSUES_WITH_COMPLETENESS:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: completeness
    params:
      custom_sql_arguments:
        - threshold_pct
        - condition
      custom_sql_statement: |-
        select *
        from
          (select sum(case when $condition then 1 else 0 end) * 100 / count(*) as pct 
          from data
          )
        where pct < $threshold_pct

rule_bindings:
  COMPLETENESS_CHECK:
    entity_id: COMPLETENESS_CHECK_OK
    column_id: DATA
    row_filter_id: NONE
    rule_ids:
      - NO_ISSUES_WITH_COMPLETENESS:
          condition: data.data IS NOT NULL
          threshold_pct: 100
```
Full example: [rule](/docs/examples/advanced_rules/completness_with_condition.yaml#57) and [rule binding](/docs/examples/advanced_rules/completness_with_condition.yaml#74)

### Uniqueness
#### Validating Uniqueness
This check is used to identify whether there are duplicates at the table level.

Note: In this particular example, the column data has to be prefixed with an alias `data` not to confict with CTE that CloudDQ uses named `data`, but the group by statement cannot contain any aliases, hence the need of two parameters.

Input parameters:
- column_names - comma separated list of column names used for the uniqueness check
- group_by_statement - same like above, but with no aliases

Example config:
```yaml
rules:
  NO_DUPLICATES_IN_COLUMN_GROUPS:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: uniqueness
    params:
      custom_sql_arguments:
        - column_names
        - group_by_statement
      custom_sql_statement: |-
        select a.*
        from data a
        inner join (
          select
            $column_names
          from data
          group by $group_by_statement
          having count(*) > 1
        ) duplicates
        using ($group_by_statement)

rule_bindings:
  UNIQUENESS_CHECK:
    entity_id: UNIQUENESS_CHECK_OK
    column_id: DATA
    row_filter_id: NONE
    rule_ids:
      - NO_DUPLICATES_IN_COLUMN_GROUPS:
          column_names: data.data, id, type
          group_by_statement: data, id, type
```
Full example: [rule](/docs/examples/advanced_rules/uniqueness_with_column_groups.yaml#77) and [rule binding](/docs/examples/advanced_rules/uniqueness_with_column_groups.yaml#98)

### Accuracy
#### Validating Accuracy
This check is used to validate whether a value is accurate and falls within an expected distribution of values.

There are two scenarios to consider:
- Scenario 1 - simple case of checking if the value falls within `('MALE', 'FEMALE')` category
- Scenario 2 - statistical case, to check if a value is not an outlier from a given distribution

Note: In this concrete example (Scenario 2), the accuracy checks is intended to validate if there's a risk of having a default value of Unix epoch (1970-01-01) for the Date Of Birth (DOB) for some specific product types. DOB does not follow the `normal distribution`, it's rather expected to be `uniform`, so it is not possible to apply the tipical statistical methods of finding outliers like [z_score](https://en.wikipedia.org/wiki/Standard_score) - which works only with numerical values. However, some heuristics can created to detect values with high spikes, not following uniform distribution.

Input parameters:
Scenario 1:
  - deviation_threshold_pct - percentage of deviation allowed from 100% accuracy

Scenario 2:
  - standard_deviation_threshold - a threshold of the permitted standard deviation value
  - bucket_ratio - a rate defined as `average bucket size / number of distinct buckets`, where bucket is a distinct column value (string type, multicategory). Bucket size means count of occurences of a specific value.

Note2: For Scenario 2, for the uniform distribution, the proposed heuristic assumes both values should remain low.

**Note3: This approach is only ilustrative, more solid rule should be established for a particular data characteristic and its distribution.**

Example config:
```yaml
rules:
  NO_ACCURACY_ISSUES_SIMPLE:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: accuracy
    params:
      custom_sql_arguments:
        - deviation_threshold_pct
      custom_sql_statement: |-
        select
          *
        from (
          select
            countif(gender in ('MALE', 'FEMALE')) as n
          from data
          )
        where
          ifnull(safe_divide(n,
              (
              select
                count(*)
              from data
              )),
            cast('inf' as float64)) < (1 - ($deviation_threshold_pct / 100))
  NO_ACCURACY_ISSUES_DISTRIBUTION:
    rule_type: CUSTOM_SQL_STATEMENT
    dimension: accuracy
    params:
      custom_sql_arguments:
        - standard_deviation_threshold
        - bucket_ratio
      custom_sql_statement: |-
        select *
        from (
          select
            stddev_pop(num_inst) as std_dev,
            avg(num_inst) as avg_bucket_size,
            any_value(dist_buckets) as dist_buckets,
            safe_divide(avg(num_inst), any_value(dist_buckets)) as bucket_ratio
          from (
            select
              dateOfBirth, count(*) over (partition by dateOfBirth) as num_inst, count(distinct dateOfBirth) over () as dist_buckets
            from data
            )
        )
        where std_dev > $standard_deviation_threshold or bucket_ratio > $bucket_ratio

rule_bindings:
  ACCURACY_CHECK_SIMPLE:
    entity_id: ACCURACY_CHECK_SIMPLE
    column_id: FULL_NAME
    row_filter_id: NONE
    rule_ids:
      - NO_ACCURACY_ISSUES_SIMPLE:
          deviation_threshold_pct: 25
  ACCURACY_CHECK_DISTRIBUTION:
    entity_id: ACCURACY_CHECK_DISTRIBUTION_OK
    column_id: FULL_NAME
    row_filter_id: NONE
    rule_ids:
      - NO_ACCURACY_ISSUES_DISTRIBUTION:
          standard_deviation_threshold: 2
          bucket_ratio: 10
```
Full example: [rule](/docs/examples/advanced_rules/accuracy_distribution_based.yaml#99) and [rule binding](/docs/examples/advanced_rules/accuracy_distribution_based.yaml#146)
