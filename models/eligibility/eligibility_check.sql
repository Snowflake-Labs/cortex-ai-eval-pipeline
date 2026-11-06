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

{%- set min_length = var('min_text_length', 20) -%}
{%- set languages = var('supported_languages', ['en']) -%}

with tickets as (
    select * from {{ ref('stg_support_tickets') }}
),

checks as (
    select
        ticket_id,
        ticket_text,
        language,
        created_at,
        category_ground_truth,
        priority,
        customer_tier,

        -- Eligibility rules
        case
            when ticket_text is null or trim(ticket_text) = ''
                then false
            when length(trim(ticket_text)) < {{ min_length }}
                then false
            when language is null or language not in ({% for lang in languages %}'{{ lang }}'{% if not loop.last %}, {% endif %}{% endfor %})
                then false
            else true
        end as is_eligible,

        case
            when ticket_text is null or trim(ticket_text) = ''
                then 'empty_or_null_text'
            when length(trim(ticket_text)) < {{ min_length }}
                then 'text_too_short'
            when language is null or language not in ({% for lang in languages %}'{{ lang }}'{% if not loop.last %}, {% endif %}{% endfor %})
                then 'unsupported_language'
            else null
        end as ineligibility_reason

    from tickets
)

select
    ticket_id,
    ticket_text,
    language,
    created_at,
    category_ground_truth,
    priority,
    customer_tier,
    is_eligible,
    ineligibility_reason,
    {{ get_run_metadata() }}
from checks
