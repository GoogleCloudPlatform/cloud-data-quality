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
    incremental_strategy = 'append',
    partition_by = {
        'field': 'execution_ts',
        'data_type': 'timestamp',
    },
    unique_key = 'dq_run_id',
    cluster_by = ['table_id', 'column_id', 'rule_binding_id', 'rule_id'],
  )
}}

with dq_summary as (

    select * from {{ ref('main') }}

)

select *
from dq_summary
