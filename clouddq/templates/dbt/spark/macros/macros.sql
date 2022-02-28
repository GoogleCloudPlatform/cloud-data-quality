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
{% if rule_configs.get("dimension") %}
    '{{ rule_configs.get("dimension") }}' AS dimension,
{% else %}
    CAST(NULL AS STRING) AS dimension,
{% endif %}
    CASE
{% if rule_configs.get("rule_type") == "NOT_NULL" %}
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% elif rule_configs.get("rule_type") == "NOT_BLANK" %}
      WHEN {{ column_name }} IS NULL THEN CAST(NULL AS BOOLEAN)
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% elif rule_configs.get("rule_type") == "CUSTOM_SQL_EXPR" %}
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% else %}
      WHEN ({{ column_name }} IS NULL OR TRIM ({{ column_name }}) = '') THEN CAST(NULL AS BOOLEAN)
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% endif %}
    ELSE
      FALSE
    END AS simple_rule_row_is_valid,
{% if rule_configs.get("rule_type") == "NOT_NULL" %}
    TRUE AS skip_null_count,
{% else %}
    FALSE AS skip_null_count,
{% endif %}
    CAST(NULL AS INTEGER) AS complex_rule_validation_errors_count,
    CAST(NULL AS BOOLEAN) AS complex_rule_validation_success_flag
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
    CAST(NULL AS STRING) AS column_id,
    CAST(NULL AS STRING)  AS column_value,
{% if rule_configs.get("dimension") %}
    '{{ rule_configs.get("dimension") }}' AS dimension,
{% else %}
    CAST(NULL AS STRING) AS dimension,
{% endif %}
    CAST(NULL AS BOOLEAN) AS simple_rule_row_is_valid,
    TRUE AS skip_null_count,
    custom_sql_statement_validation_errors.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    CASE
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count IS NULL THEN CAST(NULL AS BOOLEAN)
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count = 0 THEN TRUE
      ELSE FALSE
    END AS complex_rule_validation_success_flag
  FROM
    zero_record
  LEFT JOIN
    (
      SELECT
        '{{ rule_binding_id }}' AS _rule_binding_id,
        COUNT(*) AS complex_rule_validation_errors_count
      FROM (
      {{ rule_configs.get("rule_sql_expr") }}
      ) custom_sql
    ) custom_sql_statement_validation_errors
  ON
    zero_record.rule_binding_id = custom_sql_statement_validation_errors._rule_binding_id
{% endmacro -%}
