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
{% from 'macros.sql' import validate_simple_rule -%}
{% from 'macros.sql' import validate_complex_rule -%}
{%- macro run_dq_main(configs, environment, dq_summary_table_name, metadata, configs_hashsum, progress_watermark) -%}
{% set rule_binding_id = configs.get('rule_binding_id') -%}
{% set rule_configs_dict = configs.get('rule_configs_dict') -%}
{% set filter_sql_expr = configs.get('row_filter_configs').get('filter_sql_expr') -%}
{% set column_name = configs.get('column_configs').get('name') -%}
{% set entity_configs = configs.get('entity_configs') -%}
{% set dataplex_lake = entity_configs.get('dataplex_lake') -%}
{% set dataplex_zone = entity_configs.get('dataplex_zone') -%}
{% set instance_name = entity_configs.get('instance_name') -%}
{% set database_name = entity_configs.get('database_name') -%}
{% set table_name = entity_configs.get('table_name') -%}
{% set incremental_time_filter_column_id = configs.get('incremental_time_filter_column_id') %}
{% if environment and entity_configs.get('environment_override') -%}
  {% set env_override = entity_configs.get('environment_override') %}
  {% if env_override.get(environment|lower) %}
    {% set override_values = env_override.get(environment|lower) %}
    {% if override_values.get('table_name') -%}
        {% set table_name = override_values.get('table_name') -%}
    {% endif -%}
    {% if override_values.get('database_name') -%}
        {% set database_name = override_values.get('database_name') -%}
    {% endif -%}
    {% if override_values.get('instance_name') -%}
        {% set instance_name = override_values.get('instance_name') -%}
    {% endif -%}
  {% endif %}
{% endif -%}
{% set fully_qualified_table_name = "%s.%s.%s" % (instance_name, database_name, table_name) -%}
{% set _dummy = metadata.update(configs.get('metadata', '')) -%}
WITH
{%- if configs.get('incremental_time_filter_column') -%}
{% set time_column_id = configs.get('incremental_time_filter_column') %}
high_watermark_filter AS (
    SELECT
        IFNULL(MAX(execution_ts), TIMESTAMP("1970-01-01 00:00:00")) as high_watermark
    FROM `{{ dq_summary_table_name }}`
    WHERE table_id = '{{ fully_qualified_table_name }}'
      AND column_id = '{{ column_name }}'
      AND rule_binding_id = '{{ rule_binding_id }}'
      AND progress_watermark IS TRUE
),
{% endif %}
data AS (
    SELECT
      *,
      COUNT(1) OVER () as num_rows_validated,
    FROM
      `{{- fully_qualified_table_name -}}` d
{%- if configs.get('incremental_time_filter_column') %}
      ,high_watermark_filter
    WHERE
      CAST(d.{{ time_column_id }} AS TIMESTAMP)
          > high_watermark_filter.high_watermark
    AND
      {{ filter_sql_expr }}
{% else %}
    WHERE
      {{ filter_sql_expr }}
{% endif -%}
),
last_mod AS (
    SELECT
        project_id || '.' || dataset_id || '.' || table_id AS table_id,
        TIMESTAMP_MILLIS(last_modified_time) AS last_modified
    FROM {{ database_name }}.__TABLES__
),
validation_results AS (

{% for rule_id, rule_configs in rule_configs_dict.items() %}
    {%- if rule_configs.get('rule_type') == "CUSTOM_SQL_STATEMENT" -%}
      {{ validate_complex_rule(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name) }}
    {%- else -%}
      {{ validate_simple_rule(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name) }}
    {%- endif -%}
    {% if loop.nextitem is defined %}
    UNION ALL
    {% endif %}
{%- endfor -%}
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
    last_mod.last_modified,
    '{{ metadata|tojson }}' AS metadata_json_string,
    '{{ configs_hashsum }}' AS configs_hashsum,
{%- if dataplex_lake %}
    '{{ dataplex_lake }}' AS dataplex_lake,
{%- else %}
    CAST(NULL AS STRING) AS dataplex_lake,
{%- endif %}
{%- if dataplex_zone %}
    '{{ dataplex_zone }}' AS dataplex_zone,
{%- else %}
    CAST(NULL AS STRING) AS dataplex_zone,
{%- endif %}
    CONCAT(r.rule_binding_id, '_', r.rule_id, '_', r.execution_ts, '_', {{ progress_watermark }}) AS dq_run_id,
    {{ progress_watermark|upper }} AS progress_watermark,
  FROM
    validation_results r
  JOIN last_mod USING(table_id)
)
SELECT
  *
FROM
  all_validation_results

{%- endmacro -%}

{{-  run_dq_main(configs, environment, dq_summary_table_name, metadata, configs_hashsum, progress_watermark) -}}
