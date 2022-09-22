-- Copyright 2022 Google LLC
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
'<rule_binding_id>' AS rule_binding_id,
),
data AS (
SELECT
*,
'<rule_binding_id>' AS rule_binding_id,
FROM
`<your-gcp-project-id>.austin_311.311_service_requests` d
WHERE
True
),
last_mod AS (
SELECT
project_id || '.' || dataset_id || '.' || table_id AS table_id,
TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `<your-gcp-project-id>.austin_311.__TABLES__`
),
validation_results AS (
SELECT
CURRENT_TIMESTAMP() AS execution_ts,
'<rule_binding_id>' AS rule_binding_id,
'NOT_NULL_SIMPLE' AS rule_id,
'<your-gcp-project-id>.austin_311.311_service_requests' AS table_id,
'unique_key' AS column_id,
data.unique_key AS column_value,
CAST(NULL AS STRING) AS dimension,
CASE
WHEN unique_key IS NOT NULL THEN TRUE
ELSE
FALSE
END AS simple_rule_row_is_valid,
TRUE AS skip_null_count,
CAST(NULL AS INT64) AS complex_rule_validation_errors_count,
CAST(NULL AS BOOLEAN) AS complex_rule_validation_success_flag,
 """
 WITH
 zero_record AS (
 SELECT
 '<rule_binding_id>' AS rule_binding_id,
 ),
 data AS (
 SELECT
 *,
 '<rule_binding_id>' AS rule_binding_id,
 FROM
 `<your-gcp-project-id>.austin_311.311_service_requests` d
 WHERE
 True
 ),
 last_mod AS (
 SELECT
 project_id || '.' || dataset_id || '.' || table_id AS table_id,
 TIMESTAMP_MILLIS(last_modified_time) AS last_modified
 FROM `<your-gcp-project-id>.austin_311.__TABLES__`
 ),
 validation_results AS (SELECT
 '<rule_binding_id>' AS rule_binding_id,
 'NOT_NULL_SIMPLE' AS rule_id,
 '<your-gcp-project-id>.austin_311.311_service_requests' AS table_id,
 'unique_key' AS column_id,
 data.unique_key AS column_value,
 data.unique_key AS unique_key,
 data.complaint_description AS complaint_description,
 data.source AS source,
 data.status AS status,
 CAST(NULL AS STRING) AS dimension,
 CASE
 WHEN unique_key IS NOT NULL THEN TRUE
 ELSE
 FALSE
 END AS simple_rule_row_is_valid,
 TRUE AS skip_null_count,
 CAST(NULL AS INT64) AS complex_rule_validation_errors_count,
 CAST(NULL AS BOOLEAN) AS complex_rule_validation_success_flag,
 FROM
 zero_record
 LEFT JOIN
 data
 ON
 zero_record.rule_binding_id = data.rule_binding_id
 ),
 all_validation_results AS (
 SELECT
 '{{ invocation_id }}' as _dq_validation_invocation_id,
 r.rule_binding_id AS _dq_validation_rule_binding_id,
 r.rule_id AS _dq_validation_rule_id,
 r.column_id AS _dq_validation_column_id,
 r.column_value AS _dq_validation_column_value,
 CAST(r.dimension AS STRING) AS _dq_validation_dimension,
 r.simple_rule_row_is_valid AS _dq_validation_simple_rule_row_is_valid,
 r.complex_rule_validation_errors_count AS _dq_validation_complex_rule_validation_errors_count,
 r.complex_rule_validation_success_flag AS _dq_validation_complex_rule_validation_success_flag,
 r.unique_key AS unique_key,
 r.complaint_description AS complaint_description,
 r.source AS source,
 r.status AS status,
 FROM
 validation_results r
 )
 SELECT
 *
 FROM
 all_validation_results
 WHERE
 _dq_validation_simple_rule_row_is_valid is False
 OR
 _dq_validation_complex_rule_validation_success_flag is False
 ORDER BY _dq_validation_rule_id"""
 AS failed_records_query,
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
r.column_value AS column_value,
CAST(r.dimension AS STRING) AS dimension,
r.skip_null_count AS skip_null_count,
r.simple_rule_row_is_valid AS simple_rule_row_is_valid,
r.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
r.complex_rule_validation_success_flag AS complex_rule_validation_success_flag,
(SELECT COUNT(*) FROM data) AS rows_validated,
last_mod.last_modified,
'{"brand": "one"}' AS metadata_json_string,
'' AS configs_hashsum,
CAST(NULL AS STRING) AS dataplex_lake,
CAST(NULL AS STRING) AS dataplex_zone,
CAST(NULL AS STRING) AS dataplex_asset_id,
CONCAT(r.rule_binding_id, '_', r.rule_id, '_', r.execution_ts, '_', True) AS dq_run_id,
TRUE AS progress_watermark,
 failed_records_query AS failed_records_query,
FROM
validation_results r
JOIN last_mod USING(table_id)
)
SELECT
*
FROM
all_validation_results