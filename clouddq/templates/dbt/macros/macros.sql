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
    {{ column_name }} AS column_value,
{% if 'dimension' in rule_configs %}
    CAST('{{ rule_configs.get("dimension") }}' AS STRING) AS dimension,
{% else %}
    NULL AS dimension,
{% endif %}
    num_rows_validated AS num_rows_validated,
    CASE
{% if rule_configs.get("rule_type") == "NOT_NULL" %}
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% else %}
      WHEN {{ column_name }} IS NULL THEN NULL
      WHEN {{ rule_configs.get("rule_sql_expr") }} THEN TRUE
{% endif %}
    ELSE
      FALSE
    END AS simple_rule_row_is_valid,
    NULL AS complex_rule_validation_errors_count,
  FROM
    data
{% endmacro -%}

{% macro validate_complex_rule(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name ) -%}
  SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    '{{ rule_binding_id }}' AS rule_binding_id,
    '{{ rule_id }}' AS rule_id,
    '{{ fully_qualified_table_name }}' AS table_id,
    '{{ column_name }}' AS column_id,
    NULL AS column_value,
{% if 'dimension' in rule_configs %}
    '{{ rule_configs.get("dimension") }}' AS dimension,
{% else %}
    NULL AS dimension,
{% endif %}
    (select distinct num_rows_validated from data) as num_rows_validated,
    FALSE AS simple_rule_row_is_valid,
    COUNT(*) as complex_rule_validation_errors_count,
  FROM (
    {{ rule_configs.get("rule_sql_expr") }}
  ) custom_sql_statement_validation_errors
{% endmacro -%}
