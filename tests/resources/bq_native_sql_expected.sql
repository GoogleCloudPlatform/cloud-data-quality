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
'<rule_binding_id>' AS _internal_rule_binding_id,
),
data AS (
SELECT
*,
'<rule_binding_id>' AS _internal_rule_binding_id,
FROM
`<your-gcp-project-id>.austin_311.311_service_requests` d
WHERE
True
),
last_mod AS (
SELECT
project_id || '.' || dataset_id || '.' || table_id AS _internal_table_id,
TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `<your-gcp-project-id>.austin_311.__TABLES__`
),
validation_results AS (
SELECT
CURRENT_TIMESTAMP() AS _internal_execution_ts,
'<rule_binding_id>' AS _internal_rule_binding_id,
'NOT_NULL_SIMPLE' AS _internal_rule_id,
'<your-gcp-project-id>.austin_311.311_service_requests' AS _internal_table_id,
'unique_key' AS _internal_column_id,
data.unique_key AS _internal_column_value,
CAST(NULL AS STRING) AS _internal_dimension,
CASE
WHEN unique_key IS NOT NULL THEN TRUE
ELSE
FALSE
END AS _internal_simple_rule_row_is_valid,
TRUE AS _internal_skip_null_count,
CAST(NULL AS INT64) AS _internal_complex_rule_validation_errors_count,
CAST(NULL AS BOOLEAN) AS _internal_complex_rule_validation_success_flag,
 r"""
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
 project_id || '.' || dataset_id || '.' || table_id AS _internal_table_id,
 TIMESTAMP_MILLIS(last_modified_time) AS last_modified
 FROM `<your-gcp-project-id>.austin_311.__TABLES__`
 ),
 validation_results AS (SELECT
 '<rule_binding_id>' AS rule_binding_id,
 'NOT_NULL_SIMPLE' AS _internal_rule_id,
 '<your-gcp-project-id>.austin_311.311_service_requests' AS _internal_table_id,
 'unique_key' AS _internal_column_id,
 data.unique_key AS _internal_column_value,
 data.unique_key AS _internal_unique_key,
 data.complaint_description AS _internal_complaint_description,
 data.source AS _internal_source,
 data.status AS _internal_status,
 CAST(NULL AS STRING) AS _internal_dimension,
 CASE
 WHEN unique_key IS NOT NULL THEN TRUE
 ELSE
 FALSE
 END AS _internal_simple_rule_row_is_valid,
 TRUE AS _internal_skip_null_count,
 CAST(NULL AS INT64) AS complex_rule_validation_errors_count,
 CAST(NULL AS BOOLEAN) AS _internal_complex_rule_validation_success_flag,
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
 r._internal_rule_id AS _dq_validation_rule_id,
 r._internal_column_id AS _dq_validation_column_id,
 r._internal_column_value AS _dq_validation_column_value,
 CAST(r._internal_dimension AS STRING) AS _dq_validation_dimension,
 r._internal_simple_rule_row_is_valid AS _dq_validation_simple_rule_row_is_valid,
 r.complex_rule_validation_errors_count AS _dq_validation_complex_rule_validation_errors_count,
 r._internal_complex_rule_validation_success_flag AS _dq_validation_complex_rule_validation_success_flag,
 r._internal_unique_key AS unique_key,
 r._internal_complaint_description AS complaint_description,
 r._internal_source AS source,
 r._internal_status AS status,
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
 AS _internal_failed_records_query,
to_json(struct(data.unique_key,data.complaint_description,data.source,data.status)) AS include_reference_columns_json,
FROM
zero_record
LEFT JOIN
data
ON
zero_record._internal_rule_binding_id = data._internal_rule_binding_id
),
all_validation_results AS (
SELECT
r._internal_execution_ts AS execution_ts,
r._internal_rule_binding_id AS rule_binding_id,
r._internal_rule_id AS rule_id,
r._internal_table_id AS table_id,
r._internal_column_id AS column_id,
r._internal_column_value AS column_value,
CAST(r._internal_dimension AS STRING) AS dimension,
r._internal_skip_null_count AS skip_null_count,
r._internal_simple_rule_row_is_valid AS simple_rule_row_is_valid,
r._internal_complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
r._internal_complex_rule_validation_success_flag AS complex_rule_validation_success_flag,
(SELECT COUNT(*) FROM data) AS rows_validated,
last_mod.last_modified,
'{"brand": "one"}' AS metadata_json_string,
'' AS configs_hashsum,
CAST(NULL AS STRING) AS dataplex_lake,
CAST(NULL AS STRING) AS dataplex_zone,
CAST(NULL AS STRING) AS dataplex_asset_id,
CONCAT(r._internal_rule_binding_id, '_', r._internal_rule_id, '_', r._internal_execution_ts, '_', True) AS dq_run_id,
TRUE AS progress_watermark,
 _internal_failed_records_query AS failed_records_query,
r.include_reference_columns_json as include_reference_columns_json,
FROM
validation_results r
JOIN last_mod USING(_internal_table_id)
)
SELECT
*
FROM
all_validation_results