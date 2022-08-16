{% from 'macros.sql' import validate_simple_rule_failed_records_query -%}
{% from 'macros.sql' import validate_complex_rule_failed_records_query -%}
{%- macro create_failed_records_sql(configs, environment, dq_summary_table_name, metadata, configs_hashsum, progress_watermark, dq_summary_table_exists, high_watermark_value, current_timestamp_value, rule_id, rule_type) -%}
{% set rule_binding_id = configs.get('rule_binding_id') -%}
{% set rule_configs_dict = configs.get('rule_configs_dict') -%}
{% set rule_configs = rule_configs_dict.get(rule_id) -%}
{% set filter_sql_expr = configs.get('row_filter_configs').get('filter_sql_expr') -%}
{% set column_name = configs.get('column_configs').get('name') -%}
{% set entity_configs = configs.get('entity_configs') -%}
{% set partition_fields = entity_configs.get('partition_fields')-%}
{% set dataplex_lake = entity_configs.get('dataplex_lake') -%}
{% set dataplex_zone = entity_configs.get('dataplex_zone') -%}
{% set dataplex_asset_id = entity_configs.get('dataplex_asset_id') -%}
{% set instance_name = entity_configs.get('instance_name') -%}
{% set database_name = entity_configs.get('database_name') -%}
{% set table_name = entity_configs.get('table_name') -%}
{% set include_reference_columns =  configs.get('include_reference_columns') -%}
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
{%- if configs.get('incremental_time_filter_column') and dq_summary_table_exists -%}
{% set time_column_id = configs.get('incremental_time_filter_column') %}
{% endif %}
zero_record AS (
    SELECT
        '{{ rule_binding_id }}' AS rule_binding_id,
),
data AS (
    SELECT
      *,
      '{{ rule_binding_id }}' AS rule_binding_id,
    FROM
      `{{- fully_qualified_table_name -}}` d
{%- if configs.get('incremental_time_filter_column') and dq_summary_table_exists %}
    WHERE
      CAST(d.{{ time_column_id }} AS TIMESTAMP)
          BETWEEN CAST('{{ high_watermark_value }}' AS TIMESTAMP) AND CAST('{{ current_timestamp_value }}' AS TIMESTAMP)
    AND
      {{ filter_sql_expr }}
{% else %}
    WHERE
      {{ filter_sql_expr }}
{% endif -%}
{%- if partition_fields %}
    {% for field in partition_fields %}
        AND {{ field['name'] }} IS NOT NULL
    {%- endfor -%}
{% endif -%}
),
last_mod AS (
    SELECT
        project_id || '.' || dataset_id || '.' || table_id AS table_id,
        TIMESTAMP_MILLIS(last_modified_time) AS last_modified
    FROM `{{ instance_name }}.{{ database_name }}.__TABLES__`
),
validation_results AS (

{%- if rule_type == "CUSTOM_SQL_STATEMENT" -%}
  {{ validate_complex_rule_failed_records_query(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name, include_reference_columns) }}
{%- else -%}
  {{ validate_simple_rule_failed_records_query(rule_id, rule_configs, rule_binding_id, column_name, fully_qualified_table_name, include_reference_columns) }}
{%- endif -%}
),
all_validation_results AS (
  SELECT
    r.rule_binding_id AS _dq_validation_rule_binding_id,
    r.rule_id AS _dq_validation_rule_id,
    r.column_id AS _dq_validation_column_id,
    r.column_value AS _dq_validation_column_value,
    CAST(r.dimension AS STRING) AS _dq_validation_dimension,
    r.simple_rule_row_is_valid AS _dq_validation_simple_rule_row_is_valid,
    r.complex_rule_validation_errors_count AS _dq_validation_complex_rule_validation_errors_count,
    r.complex_rule_validation_success_flag AS _dq_validation_complex_rule_validation_success_flag,
    {% for ref_column_name in include_reference_columns %}
        r.{{ ref_column_name }} AS {{ ref_column_name }},
    {%- endfor -%}
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
ORDER BY _dq_validation_rule_id

{%- endmacro -%}

{{-  create_failed_records_sql(configs, environment, dq_summary_table_name, metadata, configs_hashsum, progress_watermark, dq_summary_table_exists, high_watermark_value, current_timestamp_value, rule_id, rule_type) -}}
