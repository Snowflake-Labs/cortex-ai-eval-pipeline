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

-- A run must never PASS the gate while a critical metric is missing or NULL.
-- Returns offending (run_id, metric_name) pairs; the test fails if any rows come back.

{{ config(tags=['runtime']) }}

select
    g.run_id,
    t.metric_name
from {{ ref('gate_decision') }} g
cross join {{ ref('evaluation_thresholds') }} t
left join {{ ref('eval_aggregate') }} m
    on m.run_id = g.run_id
    and m.metric_name = t.metric_name
where t.is_critical
    and m.metric_value is null
    and g.gate_result = 'PASS'
