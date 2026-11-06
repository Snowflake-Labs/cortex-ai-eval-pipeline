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

-- Every classifier output must reach eval_row_level, and an invalid output
-- (label outside the taxonomy incl. error sentinels, confidence outside [0, 1],
-- or empty reasoning) must be marked invalid and flagged, never judged as
-- valid. Returns offending rows; the test fails if any come back.

with predictions as (
    select
        ticket_id,
        run_id,
        (
            coalesce(predicted_category in (
                {%- for c in var('categories') %}'{{ c }}'{{ ', ' if not loop.last }}{% endfor -%}
            ), false)
            and coalesce(confidence between 0 and 1, false)
            and coalesce(trim(reasoning) != '', false)
        ) as expected_valid
    from {{ ref('classify_ticket') }}
)

select
    p.ticket_id,
    p.expected_valid,
    e.is_valid_output,
    e.judge_flagged
from predictions p
left join {{ ref('eval_row_level') }} e
    on e.ticket_id = p.ticket_id
    and e.run_id = p.run_id
where e.ticket_id is null
    or e.is_valid_output != p.expected_valid
    or (not p.expected_valid and not e.judge_flagged)
