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

{%- set languages = var('supported_languages', ['en']) -%}

with tickets as (
    select * from {{ ref('stg_support_tickets') }}
),

metrics as (
    select
        {{ get_run_metadata() }},
        count(*) as total_rows,
        count(case when ticket_text is null or trim(ticket_text) = '' then 1 end) as null_or_empty_count,
        round(count(case when ticket_text is null or trim(ticket_text) = '' then 1 end)::float / nullif(count(*), 0), 4) as null_rate,
        count(*) - count(distinct ticket_id) as duplicate_count,
        round((count(*) - count(distinct ticket_id))::float / nullif(count(*), 0), 4) as duplicate_rate,
        -- length stats cover non-NULL text only (NULLs are counted in null_or_empty_count)
        round(avg(length(ticket_text)), 1) as avg_text_length,
        min(length(ticket_text)) as min_text_length,
        max(length(ticket_text)) as max_text_length,
        count(distinct language) as language_count,
        -- same rule as eligibility_check: NULL or not in var('supported_languages')
        count(case when language is null or language not in ({% for lang in languages %}'{{ lang }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 1 end) as unsupported_language_count,
        count(distinct category_ground_truth) as category_count,
        min(created_at) as earliest_ticket,
        max(created_at) as latest_ticket
    from tickets
)

select * from metrics
