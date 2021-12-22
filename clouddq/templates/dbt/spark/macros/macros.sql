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

{% macro validate_simple_rule(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name, resource_type ) -%}
  SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    '{{ rule_binding_id }}' AS rule_binding_id,
    '{{ rule_id }}' AS rule_id,
    '{{ fully_qualified_table_name }}' AS table_id,
    '{{ column_name }}' AS column_id,
    {{ column_name }} AS column_value,
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
{% if resource_type == "STORAGE_BUCKET" %}
    CAST(NULL AS STRING) AS complex_rule_validation_errors_count
{% else %}
    NULL AS complex_rule_validation_errors_count
{% endif %}

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
    {{ column_name }} AS column_value,
    (select distinct num_rows_validated from data) as num_rows_validated,
    FALSE AS simple_rule_row_is_valid,
    COUNT(1) OVER() as complex_rule_validation_errors_count
  FROM (
    {{ rule_configs.get("rule_sql_expr") }}
  ) custom_sql_statement_validation_errors
{% endmacro -%}

{% macro generate_table_name(resource_type, instance_name, database_name, table_name) -%}
    {%- if resource_type == "STORAGE_BUCKET" -%}
        {{ database_name }}.{{ table_name }}
    {%- else -%}
        {{ instance_name }}.{{ database_name }}.{{ table_name }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_dq_run_id(resource_type, progress_watermark) -%}
    {%- if resource_type == "STORAGE_BUCKET" -%}
        CONCAT(r.rule_binding_id, '_', r.rule_id, '_', to_utc_timestamp(to_date(r.execution_ts), 'US/Pacific'), '_', {{ progress_watermark }}) AS dq_run_id,
    {%- else -%}
        CONCAT(r.rule_binding_id, '_', r.rule_id, '_', TIMESTAMP_TRUNC(r.execution_ts, HOUR), '_', {{ progress_watermark }}) AS dq_run_id,
    {%- endif -%}
{%- endmacro %}
