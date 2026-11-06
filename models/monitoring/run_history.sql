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
        unique_key='run_id',
        incremental_strategy='merge',
        full_refresh=false
    )
}}

with eligibility as (
    select * from {{ ref('eligibility_check') }}
),

inference as (
    select * from {{ ref('classify_ticket') }}
),

aggregate as (
    select * from {{ ref('eval_aggregate') }}
),

gate as (
    select * from {{ ref('gate_decision') }}
),

eligibility_counts as (
    select
        count(*) as input_row_count,
        count(case when is_eligible then 1 end) as eligible_count
    from eligibility
),

inference_counts as (
    select count(*) as inference_count
    from inference
),

inference_meta as (
    select model_name, prompt_version
    from inference
    order by inference_timestamp desc
    limit 1
),

judge_flag as (
    select run_id, metric_value as judge_flag_rate
    from aggregate
    where metric_name = 'judge_flag_rate'
),

invalid_output as (
    select run_id, metric_value as invalid_output_rate
    from aggregate
    where metric_name = 'invalid_output_rate'
),

eligible_coverage as (
    select run_id, metric_value as eligible_coverage
    from aggregate
    where metric_name = 'eligible_coverage'
),

run_summary as (
    select
        g.run_id,
        g.evaluated_at as run_timestamp,
        ec.input_row_count,
        ec.eligible_count,
        ic.inference_count,
        jf.judge_flag_rate,
        io.invalid_output_rate,
        ecc.eligible_coverage,
        g.gate_result,
        g.total_failures,
        g.critical_failures,
        im.model_name,
        im.prompt_version
    from gate g
    cross join eligibility_counts ec
    cross join inference_counts ic
    cross join inference_meta im
    left join judge_flag jf on g.run_id = jf.run_id
    left join invalid_output io on g.run_id = io.run_id
    left join eligible_coverage ecc on g.run_id = ecc.run_id
)

select * from run_summary
