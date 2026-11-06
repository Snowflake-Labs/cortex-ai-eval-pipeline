-- Copyright 2026 Snowflake Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{{
    config(
        materialized='incremental',
        unique_key=['ticket_id', 'run_id'],
        incremental_strategy='merge',
        full_refresh=false
    )
}}

{#
  Append-only publication log: each run's predictions are added only if the
  run PASSed the gate. A FAIL run adds nothing, so earlier published batches
  are kept. Consumers wanting one row per ticket should use current_predictions.
#}

with predictions as (
    select * from {{ ref('classify_ticket') }}
),

gate as (
    select * from {{ ref('gate_decision') }}
)

select
    p.ticket_id,
    p.predicted_category,
    p.confidence,
    p.reasoning,
    p.model_name,
    p.prompt_version,
    p.input_fingerprint,
    p.run_id,
    p.inference_timestamp,
    g.gate_result,
    current_timestamp() as published_at
from predictions p
inner join gate g
    on p.run_id = g.run_id
where g.gate_result = 'PASS'
