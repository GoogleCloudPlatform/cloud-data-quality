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

SELECT
    execution_ts,
    rule_binding_id,
    rule_id,
    table_id,
    column_id,
    dimension,
    metadata_json_string,
    configs_hashsum,
    dataplex_lake,
    dataplex_zone,
    dataplex_asset_id,
    dq_run_id,
    progress_watermark,
    rows_validated,
    complex_rule_validation_errors_count,
    complex_rule_validation_success_flag,
    last_modified,
    skip_null_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS TRUE)
    END
    AS success_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS TRUE) / rows_validated
    END
    AS success_percentage,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS FALSE)
    END
    AS failed_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS FALSE) / rows_validated
    END
    AS failed_percentage,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        WHEN skip_null_count IS TRUE THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS NULL)
    END
    AS null_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        WHEN skip_null_count IS TRUE THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS NULL) / rows_validated
    END
    AS null_percentage,
    """rule_binding_id_1_failed_records_sql_string""" as failed_records_query,
FROM
    {{ ref('rule_binding_id_1') }}
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,25
UNION ALL
SELECT
    execution_ts,
    rule_binding_id,
    rule_id,
    table_id,
    column_id,
    dimension,
    metadata_json_string,
    configs_hashsum,
    dataplex_lake,
    dataplex_zone,
    dataplex_asset_id,
    dq_run_id,
    progress_watermark,
    rows_validated,
    complex_rule_validation_errors_count,
    complex_rule_validation_success_flag,
    last_modified,
    skip_null_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS TRUE)
    END
    AS success_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS TRUE) / rows_validated
    END
    AS success_percentage,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS FALSE)
    END
    AS failed_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS FALSE) / rows_validated
    END
    AS failed_percentage,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        WHEN skip_null_count IS TRUE THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS NULL)
    END
    AS null_count,
    CASE 
        WHEN rows_validated = 0 THEN NULL
        WHEN complex_rule_validation_errors_count IS NOT NULL THEN NULL
        WHEN skip_null_count IS TRUE THEN NULL
        ELSE COUNTIF(simple_rule_row_is_valid IS NULL) / rows_validated
    END
    AS null_percentage,
    """rule_binding_id_2_failed_records_sql_string""" as failed_records_query,
FROM
    {{ ref('rule_binding_id_2') }}
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,25