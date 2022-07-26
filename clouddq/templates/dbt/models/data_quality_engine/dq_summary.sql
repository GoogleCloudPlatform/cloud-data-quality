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

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    partition_by = {
        'field': 'execution_ts',
        'data_type': 'timestamp',
    },
    unique_key = 'dq_run_id',
    cluster_by = ['table_id', 'column_id', 'rule_binding_id', 'rule_id'],
  )
}}

with dq_summary as (

    select
      invocation_id,
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
      success_count,
      success_percentage,
      failed_count,
      failed_percentage,
      null_count,
      null_percentage,
      failed_records_query,
    from {{ ref('main') }}

)

select *
from dq_summary
