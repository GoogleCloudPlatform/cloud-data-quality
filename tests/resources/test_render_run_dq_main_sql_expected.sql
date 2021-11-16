-- Copyright 2021 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

WITH
data AS (
    SELECT
      *,
      COUNT(1) OVER () as num_rows_validated,
    FROM
      `<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details` d
    WHERE
      contact_type = 'email'
),
validation_results AS (

SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T2_DQ_1_EMAIL' AS rule_binding_id,
    'NOT_NULL_SIMPLE' AS rule_id,
    '<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details' AS table_id,
    'value' AS column_id,
    value AS column_value,
    num_rows_validated AS num_rows_validated,
    CASE

      WHEN value IS NOT NULL THEN TRUE

    ELSE
      FALSE
    END AS simple_rule_row_is_valid,
    NULL AS complex_rule_validation_errors_count,
  FROM
    data

    UNION ALL
    SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T2_DQ_1_EMAIL' AS rule_binding_id,
    'REGEX_VALID_EMAIL' AS rule_id,
    '<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details' AS table_id,
    'value' AS column_id,
    value AS column_value,
    num_rows_validated AS num_rows_validated,
    CASE

      WHEN value IS NULL THEN NULL
      WHEN REGEXP_CONTAINS( CAST( value  AS STRING), '^[^@]+[@]{1}[^@]+$' ) THEN TRUE

    ELSE
      FALSE
    END AS simple_rule_row_is_valid,
    NULL AS complex_rule_validation_errors_count,
  FROM
    data

    UNION ALL
    SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T2_DQ_1_EMAIL' AS rule_binding_id,
    'CUSTOM_SQL_LENGTH_LE_30' AS rule_id,
    '<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details' AS table_id,
    'value' AS column_id,
    value AS column_value,
    num_rows_validated AS num_rows_validated,
    CASE

      WHEN value IS NULL THEN NULL
      WHEN LENGTH( value ) <= 30 THEN TRUE

    ELSE
      FALSE
    END AS simple_rule_row_is_valid,
    NULL AS complex_rule_validation_errors_count,
  FROM
    data
    UNION ALL
  SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T2_DQ_1_EMAIL' AS rule_binding_id,
    'NOT_BLANK' AS rule_id,
    '<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details' AS table_id,
    'value' AS column_id,
    value AS column_value,
    num_rows_validated AS num_rows_validated,
    CASE

      WHEN value IS NULL THEN NULL
      WHEN TRIM(value) != '' THEN TRUE

    ELSE
    FALSE
    END AS simple_rule_row_is_valid,
    NULL AS complex_rule_validation_errors_count,
    FROM
    data
),
all_validation_results AS (
  SELECT
    r.execution_ts AS execution_ts,
    r.rule_binding_id AS rule_binding_id,
    r.rule_id AS rule_id,
    r.table_id AS table_id,
    r.column_id AS column_id,
    r.simple_rule_row_is_valid AS simple_rule_row_is_valid,
    r.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    r.column_value AS column_value,
    r.num_rows_validated AS rows_validated,
    '{"brand": "one"}' AS metadata_json_string,
    '' AS configs_hashsum,
    CONCAT(r.rule_binding_id, '_', r.rule_id, '_', TIMESTAMP_TRUNC(r.execution_ts, HOUR), '_', True) AS dq_run_id,
    TRUE AS progress_watermark,
  FROM
    validation_results r
)
SELECT
  *
FROM
  all_validation_results
