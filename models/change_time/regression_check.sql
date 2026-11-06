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

{#
  Regression check: compares current run metrics against stored baselines.
  Used in CI (offline evaluation) to detect if a code/prompt/model change
  has degraded quality below acceptable tolerance.

  Supports both 'minimum' (higher is better, e.g. accuracy) and 'maximum'
  (lower is better, e.g. error rate) threshold types.

  Every baseline is checked: a baselined metric with no current value
  (renamed, dropped, or NULL) is reported as MISSING and treated as a failure.
#}

with current_metrics as (
    select
        metric_name,
        metric_value
    from {{ ref('eval_aggregate') }}
),

baseline as (
    select
        metric_name,
        baseline_value,
        tolerance,
        threshold_type,
        last_updated
    from {{ ref('baseline_metrics') }}
),

comparison as (
    select
        b.metric_name,
        c.metric_value as current_value,
        b.baseline_value,
        b.tolerance,
        b.threshold_type,
        b.last_updated as baseline_last_updated,
        case
            when c.metric_value is null then 'MISSING'
            when b.threshold_type = 'minimum'
                and c.metric_value >= (b.baseline_value - b.tolerance) then 'PASS'
            when b.threshold_type = 'maximum'
                and c.metric_value <= (b.baseline_value + b.tolerance) then 'PASS'
            else 'REGRESSION'
        end as regression_status,
        round(c.metric_value - b.baseline_value, 4) as delta,
        case
            when b.threshold_type = 'minimum'
                then round(b.baseline_value - b.tolerance, 4)
            else round(b.baseline_value + b.tolerance, 4)
        end as boundary_value
    from baseline b
    left join current_metrics c on c.metric_name = b.metric_name
)

select
    *,
    case
        when regression_status = 'MISSING'
            then 'No current value for baselined metric ' || metric_name
                 || ' (renamed, dropped, or NULL)'
        when regression_status = 'REGRESSION' and threshold_type = 'minimum'
            then 'Current ' || current_value || ' is below minimum ' || boundary_value
                 || ' (baseline ' || baseline_value || ' - tolerance ' || tolerance || ')'
        when regression_status = 'REGRESSION' and threshold_type = 'maximum'
            then 'Current ' || current_value || ' exceeds maximum ' || boundary_value
                 || ' (baseline ' || baseline_value || ' + tolerance ' || tolerance || ')'
        else null
    end as regression_detail,
    -- dbt invocation that built this table; check_gate.py filters on it so a
    -- gate never reads rows written by another CI run
    '{{ invocation_id }}' as run_id,
    current_timestamp() as checked_at
from comparison
