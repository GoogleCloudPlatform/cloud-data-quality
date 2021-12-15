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
zero_record AS (
  SELECT
    'T3_DQ_1_EMAIL_DUPLICATE' AS rule_binding_id,
),
data AS (
    SELECT
      *,
      COUNT(1) OVER () as num_rows_validated,
      COUNT(1) OVER () - COUNT(value) OVER () as num_null_rows,
      'T3_DQ_1_EMAIL_DUPLICATE' AS rule_binding_id,
      TO_JSON_STRING(d) as row_json,
      SHA256(TO_JSON_STRING(d)) as row_json_sha256sum
    FROM
      `<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details` d
    WHERE
      contact_type = 'email'
),
last_mod AS (
    SELECT
        project_id || '.' || dataset_id || '.' || table_id AS table_id,
        TIMESTAMP_MILLIS(last_modified_time) AS last_modified
    FROM `<your_gcp_project_id>.<your_bigquery_dataset_id>.__TABLES__`
),
validation_results AS (

SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T3_DQ_1_EMAIL_DUPLICATE' AS rule_binding_id,
    'NO_DUPLICATES_IN_COLUMN_GROUPS' AS rule_id,
    '<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details' AS table_id,
    'value' AS column_id,
    custom_sql_statement_validation_errors.row_json AS column_value,
    custom_sql_statement_validation_errors.row_json_sha256sum AS row_json_sha256sum,
    CAST(NULL AS STRING) AS dimension,
    (select distinct num_rows_validated from data) as num_rows_validated,
    NULL as num_null_rows,
    custom_sql_statement_validation_errors.complex_rule_row_is_row_valid AS row_is_valid,
    COUNTIF(custom_sql_statement_validation_errors.complex_rule_row_is_row_valid IS FALSE) OVER () AS complex_rule_validation_errors_count,
    FROM
    zero_record
    LEFT JOIN
    (
      SELECT 
        'T3_DQ_1_EMAIL_DUPLICATE' AS _rule_binding_id, 
        data.row_json,
        data.row_json_sha256sum,
        CASE WHEN custom_sql.row_json_sha256sum IS NULL THEN TRUE ELSE FALSE END as complex_rule_row_is_row_valid
      FROM 
        data
      LEFT JOIN 
      (
        select a.*
        from data a
        inner join (
          select
            contact_type,value
          from data
          group by contact_type,value
          having count(*) > 1
        ) duplicates
        using (contact_type,value)
      ) custom_sql
      ON data.row_json_sha256sum = custom_sql.row_json_sha256sum
    ) custom_sql_statement_validation_errors
    ON
    zero_record.rule_binding_id = custom_sql_statement_validation_errors._rule_binding_id

    UNION ALL
    SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T3_DQ_1_EMAIL_DUPLICATE' AS rule_binding_id,
    'NOT_NULL_SIMPLE' AS rule_id,
    '<your_gcp_project_id>.<your_bigquery_dataset_id>.contact_details' AS table_id,
    'value' AS column_id,
    data.value AS column_value,
    data.row_json_sha256sum AS row_json_sha256sum,
    CAST(NULL AS STRING) AS dimension,
    data.num_rows_validated AS num_rows_validated,
    NULL AS num_null_rows,
    CASE

      WHEN value IS NOT NULL THEN TRUE

    ELSE
      FALSE
    END AS row_is_valid,
    NULL AS complex_rule_validation_errors_count,
  FROM
    zero_record
  LEFT JOIN
    data
  ON
    zero_record.rule_binding_id = data.rule_binding_id
),
all_validation_results AS (
  SELECT
    r.execution_ts AS execution_ts,
    r.rule_binding_id AS rule_binding_id,
    r.rule_id AS rule_id,
    r.table_id AS table_id,
    r.column_id AS column_id,
    CAST(r.dimension AS STRING) AS dimension,
    r.row_is_valid AS row_is_valid,
    r.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    r.column_value AS column_value,
    IFNULL(r.num_rows_validated, 0) AS rows_validated,
    r.row_json_sha256sum as row_json_sha256sum,
    r.num_null_rows,
    last_mod.last_modified,
    '{"brand": "one"}' AS metadata_json_string,
    '' AS configs_hashsum,
    CAST(NULL AS STRING) AS dataplex_lake,
    CAST(NULL AS STRING) AS dataplex_zone,
    CAST(NULL AS STRING) AS dataplex_asset_id,
    CONCAT(r.rule_binding_id, '_', r.rule_id, '_', r.execution_ts, '_', True) AS dq_run_id,
    TRUE AS progress_watermark,
  FROM
    validation_results r
  JOIN last_mod USING(table_id)
)
SELECT
  *
FROM
  all_validation_results
