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
`<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset` d
WHERE
contact_type = 'email'
AND dt IS NOT NULL),
last_mod AS (
SELECT
project_id || '.' || dataset_id || '.' || table_id AS _internal_table_id,
TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `<your-gcp-project-id>.<your_dataplex_zone_name>.__TABLES__`
),
validation_results AS (
SELECT
CURRENT_TIMESTAMP() AS _internal_execution_ts,
'<rule_binding_id>' AS _internal_rule_binding_id,
'NO_DUPLICATES_IN_COLUMN_GROUPS' AS _internal_rule_id,
'<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset' AS _internal_table_id,
CAST(NULL AS STRING) AS _internal_column_id,
NULL AS _internal_column_value,
CAST(NULL AS STRING) AS _internal_dimension,
CAST(NULL AS BOOLEAN) AS _internal_simple_rule_row_is_valid,
TRUE AS _internal_skip_null_count,
custom_sql_statement_validation_errors._internal_complex_rule_validation_errors_count AS _internal_complex_rule_validation_errors_count,
CASE
WHEN custom_sql_statement_validation_errors._internal_complex_rule_validation_errors_count IS NULL THEN TRUE
WHEN custom_sql_statement_validation_errors._internal_complex_rule_validation_errors_count = 0 THEN TRUE
ELSE FALSE
END AS _internal_complex_rule_validation_success_flag,
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
`<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset` d
WHERE
contact_type = 'email'
AND dt IS NOT NULL),
last_mod AS (
SELECT
project_id || '.' || dataset_id || '.' || table_id AS _internal_table_id,
TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `<your-gcp-project-id>.<your_dataplex_zone_name>.__TABLES__`
),
validation_results AS (SELECT
'<rule_binding_id>' AS rule_binding_id,
'NO_DUPLICATES_IN_COLUMN_GROUPS' AS _internal_rule_id,
'<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset' AS _internal_table_id,
CAST(NULL AS STRING) AS _internal_column_id,
NULL AS _internal_column_value,
custom_sql_statement_validation_errors,
CAST(NULL AS STRING) AS _internal_dimension,
CAST(NULL AS BOOLEAN) AS _internal_simple_rule_row_is_valid,
TRUE AS _internal_skip_null_count,
custom_sql_statement_validation_errors.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
CASE
WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count IS NULL THEN TRUE
WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count = 0 THEN TRUE
ELSE FALSE
END AS _internal_complex_rule_validation_success_flag,
FROM
zero_record
LEFT JOIN
(
SELECT
*,
'<rule_binding_id>' AS _rule_binding_id,
COUNT(*) OVER() AS complex_rule_validation_errors_count,
FROM (
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
) custom_sql_statement_validation_errors
ON
zero_record.rule_binding_id = custom_sql_statement_validation_errors._rule_binding_id
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
r.custom_sql_statement_validation_errors,
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
FROM
zero_record
LEFT JOIN
(
SELECT
*,
'<rule_binding_id>' AS _rule_binding_id,
COUNT(*) OVER() AS _internal_complex_rule_validation_errors_count,
FROM (
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
) custom_sql_statement_validation_errors
ON
zero_record._internal_rule_binding_id = custom_sql_statement_validation_errors._rule_binding_id
UNION ALL
SELECT
CURRENT_TIMESTAMP() AS _internal_execution_ts,
'<rule_binding_id>' AS _internal_rule_binding_id,
'NOT_NULL_SIMPLE' AS _internal_rule_id,
'<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset' AS _internal_table_id,
'value' AS _internal_column_id,
data.value AS _internal_column_value,
CAST(NULL AS STRING) AS _internal_dimension,
CASE
WHEN value IS NOT NULL THEN TRUE
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
`<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset` d
WHERE
contact_type = 'email'
AND dt IS NOT NULL),
last_mod AS (
SELECT
project_id || '.' || dataset_id || '.' || table_id AS _internal_table_id,
TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `<your-gcp-project-id>.<your_dataplex_zone_name>.__TABLES__`
),
validation_results AS (SELECT
'<rule_binding_id>' AS rule_binding_id,
'NOT_NULL_SIMPLE' AS _internal_rule_id,
'<your-gcp-project-id>.<your_dataplex_zone_name>.partitioned_gcs_asset' AS _internal_table_id,
'value' AS _internal_column_id,
data.value AS _internal_column_value,
data.row_id AS _internal_row_id,
data.contact_type AS _internal_contact_type,
data.value AS _internal_value,
CAST(NULL AS STRING) AS _internal_dimension,
CASE
WHEN value IS NOT NULL THEN TRUE
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
r._internal_row_id AS row_id,
r._internal_contact_type AS contact_type,
r._internal_value AS value,
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
'<your_dataplex_lake_id>' AS dataplex_lake,
'<your_dataplex_zone_name>' AS dataplex_zone,
'<your_dataplex_asset_id>' AS dataplex_asset_id,
CONCAT(r._internal_rule_binding_id, '_', r._internal_rule_id, '_', r._internal_execution_ts, '_', True) AS dq_run_id,
TRUE AS progress_watermark,
_internal_failed_records_query AS failed_records_query,
FROM
validation_results r
JOIN last_mod USING(_internal_table_id)
)
SELECT
*
FROM
all_validation_results