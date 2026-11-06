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

{{ config(materialized='table') }}

with metrics as (
    select * from {{ ref('eval_aggregate') }}
),

thresholds as (
    select * from {{ ref('evaluation_thresholds') }}
),

runs as (
    select distinct run_id from {{ ref('classify_ticket') }}
),

-- Check every configured threshold for every run; a missing or NULL metric fails
checks as (
    select
        r.run_id,
        t.metric_name,
        m.metric_value,
        t.threshold_value,
        t.threshold_type,
        t.is_critical,
        case
            when m.metric_value is null then 'FAIL'
            when t.threshold_type = 'minimum' and m.metric_value >= t.threshold_value then 'PASS'
            when t.threshold_type = 'maximum' and m.metric_value <= t.threshold_value then 'PASS'
            else 'FAIL'
        end as check_result
    from runs r
    cross join thresholds t
    left join metrics m
        on m.run_id = r.run_id
        and m.metric_name = t.metric_name
),

-- Aggregate to run-level decision
gate_result as (
    select
        run_id,
        case
            when count(case when check_result = 'FAIL' and is_critical then 1 end) > 0 then 'FAIL'
            else 'PASS'
        end as gate_result,
        array_agg(
            case when check_result = 'FAIL'
            then object_construct(
                'metric', metric_name,
                'value', metric_value,
                'threshold', threshold_value,
                'type', threshold_type
            )
            end
        ) as failed_checks,
        count(case when check_result = 'FAIL' then 1 end) as total_failures,
        count(case when check_result = 'FAIL' and is_critical then 1 end) as critical_failures,
        current_timestamp() as evaluated_at
    from checks
    group by run_id
)

select * from gate_result
