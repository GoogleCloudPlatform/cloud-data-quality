WITH
zero_record AS (
    SELECT
        'T14_URI_BQ_PARTITIONED_EMAIL_DUPLICATE' AS rule_binding_id,
),
data AS (
    SELECT
      *,
      'T14_URI_BQ_PARTITIONED_EMAIL_DUPLICATE' AS rule_binding_id,
    FROM
      `dataplex-clouddq.austin_311.contact_details_partitioned` d
    WHERE
      contact_type = 'email'

    
        AND ts IS NOT NULL),
last_mod AS (
    SELECT
        project_id || '.' || dataset_id || '.' || table_id AS table_id,
        TIMESTAMP_MILLIS(last_modified_time) AS last_modified
    FROM `dataplex-clouddq.austin_311.__TABLES__`
),
validation_results AS (

SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T14_URI_BQ_PARTITIONED_EMAIL_DUPLICATE' AS rule_binding_id,
    'NO_DUPLICATES_IN_COLUMN_GROUPS' AS rule_id,
    'dataplex-clouddq.austin_311.contact_details_partitioned' AS table_id,
    CAST(NULL AS STRING) AS column_id,
    NULL AS column_value,

    CAST(NULL AS STRING) AS dimension,

    CAST(NULL AS BOOLEAN) AS simple_rule_row_is_valid,
    TRUE AS skip_null_count,
    custom_sql_statement_validation_errors.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    CASE
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count IS NULL THEN CAST(NULL AS BOOLEAN)
      WHEN custom_sql_statement_validation_errors.complex_rule_validation_errors_count = 0 THEN TRUE
      ELSE FALSE
    END AS complex_rule_validation_success_flag,
  FROM
    zero_record
  LEFT JOIN
    (
      SELECT 
        'T14_URI_BQ_PARTITIONED_EMAIL_DUPLICATE' AS _rule_binding_id,
        COUNT(*) AS complex_rule_validation_errors_count,
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

    UNION ALL
    SELECT
    CURRENT_TIMESTAMP() AS execution_ts,
    'T14_URI_BQ_PARTITIONED_EMAIL_DUPLICATE' AS rule_binding_id,
    'NOT_NULL_SIMPLE' AS rule_id,
    'dataplex-clouddq.austin_311.contact_details_partitioned' AS table_id,
    'value' AS column_id,
    data.value AS column_value,

    CAST(NULL AS STRING) AS dimension,

    CASE

      WHEN value IS NOT NULL THEN TRUE

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
    r.execution_ts AS execution_ts,
    r.rule_binding_id AS rule_binding_id,
    r.rule_id AS rule_id,
    r.table_id AS table_id,
    r.column_id AS column_id,
    CAST(r.dimension AS STRING) AS dimension,
    r.skip_null_count AS skip_null_count,
    r.simple_rule_row_is_valid AS simple_rule_row_is_valid,
    r.complex_rule_validation_errors_count AS complex_rule_validation_errors_count,
    r.complex_rule_validation_success_flag AS complex_rule_validation_success_flag,
    r.column_value AS column_value,
    (SELECT COUNT(*) FROM data) AS rows_validated,
    last_mod.last_modified,
    '{"brand": "one"}' AS metadata_json_string,
    '75e1c2c9d78b7a9c38b881262f8cc0bd6c7133069681343ca037bb0a62dbcfe1' AS configs_hashsum,
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
