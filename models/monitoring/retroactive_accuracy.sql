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

{{ config(materialized='table', tags=['monitoring']) }}

{#
  Monitoring: retroactive accuracy from delayed labels.
  After the runtime gate passes and predictions are published,
  human labels arrive days later. This model joins them to every
  published batch and reports accuracy per run / model / prompt_version,
  so quality can be compared across releases.
#}

with predictions as (
    select ticket_id, predicted_category, run_id, model_name, prompt_version
    from {{ ref('published_predictions') }}
),

labels as (
    select ticket_id, human_label, labeled_at
    from {{ ref('delayed_labels') }}
),

comparison as (
    select
        p.run_id,
        p.model_name,
        p.prompt_version,
        l.labeled_at,
        case
            when lower(trim(p.predicted_category)) = lower(trim(l.human_label)) then true
            else false
        end as is_correct
    from predictions p
    inner join labels l on p.ticket_id = l.ticket_id
)

select
    run_id,
    model_name,
    prompt_version,
    count(*) as labeled_count,
    count_if(is_correct) as correct_count,
    round(correct_count / nullif(labeled_count, 0), 4) as retroactive_accuracy,
    max(labeled_at) as latest_label_at
from comparison
group by run_id, model_name, prompt_version
