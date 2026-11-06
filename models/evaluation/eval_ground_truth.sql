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

{{ config(materialized='table', schema='offline', tags=['change_time']) }}

with predictions as (
    select
        c.ticket_id,
        c.predicted_category,
        c.confidence,
        c.run_id,
        e.category_ground_truth as expected_category
    from {{ ref('classify_ticket') }} c
    inner join {{ ref('eligibility_check') }} e
        on c.ticket_id = e.ticket_id
    where e.category_ground_truth is not null
)

select
    ticket_id,
    predicted_category,
    expected_category,
    confidence,
    case
        when lower(trim(predicted_category)) = lower(trim(expected_category)) then true
        else false
    end as is_correct,
    run_id
from predictions
