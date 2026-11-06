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

{{ config(materialized='view') }}

select
    ticket_id,
    ticket_text,
    language,
    created_at::date as created_at,
    category_ground_truth,
    priority,
    customer_tier
{% if target.name == 'ci' %}
from {{ ref('golden_tickets') }}
{% else %}
from {{ ref('production_tickets') }}
{% endif %}
