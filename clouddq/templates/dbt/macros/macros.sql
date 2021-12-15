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

{% macro validate_simple_rule(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name ) -%}
  SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    '{{ rule_binding_id }}' AS rule_binding_id,
    '{{ rule_id }}' AS rule_id,
    '{{ fully_qualified_table_name }}' AS table_id,
    '{{ column_name }}' AS column_id,
    data.{{ column_name }} AS column_value,
    data.row_json_sha256sum AS row_json_sha256sum,
{% if rule_configs.get("dimension") %}
    '{{ rule_configs.get("dimension") }}' AS dimension,
{% else %}
    CAST(NULL AS STRING) AS dimension,
{% endif %}
    data.num_rows_validated AS num_rows_validated,
{% if rule_configs.get('rule_type') == "NOT_NULL" %}
    NULL AS num_null_rows,
{% else  %}
    data.num_null_rows AS num_null_rows,
{% endif %}
    CASE
{% if rule_configs.get("rule_type") == "NOT_NULL" %}
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% else %}
      WHEN {{ column_name }} IS NULL THEN NULL
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% endif %}
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
{% endmacro -%}

{% macro validate_complex_rule(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name ) -%}
  SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    '{{ rule_binding_id }}' AS rule_binding_id,
    '{{ rule_id }}' AS rule_id,
    '{{ fully_qualified_table_name }}' AS table_id,
    '{{ column_name }}' AS column_id,
    custom_sql_statement_validation_errors.row_json AS column_value,
    custom_sql_statement_validation_errors.row_json_sha256sum AS row_json_sha256sum,
{% if rule_configs.get("dimension") %}
    '{{ rule_configs.get("dimension") }}' AS dimension,
{% else %}
    CAST(NULL AS STRING) AS dimension,
{% endif %}
    (select distinct num_rows_validated from data) as num_rows_validated,
    NULL as num_null_rows,
    custom_sql_statement_validation_errors.complex_rule_row_is_row_valid AS row_is_valid,
    COUNTIF(custom_sql_statement_validation_errors.complex_rule_row_is_row_valid IS FALSE) OVER () AS complex_rule_validation_errors_count,
  FROM
    zero_record
  LEFT JOIN
    (
      SELECT 
        '{{ rule_binding_id }}' AS _rule_binding_id, 
      data.row_json,
      data.row_json_sha256sum,
      CASE WHEN custom_sql.row_json_sha256sum IS NULL THEN TRUE ELSE FALSE END as complex_rule_row_is_row_valid
      FROM 
        data
      LEFT JOIN 
      (
      {{ rule_configs.get("rule_sql_expr") }}
      ) custom_sql
      ON data.row_json_sha256sum = custom_sql.row_json_sha256sum
    ) custom_sql_statement_validation_errors
  ON
    zero_record.rule_binding_id = custom_sql_statement_validation_errors._rule_binding_id
{% endmacro -%}
