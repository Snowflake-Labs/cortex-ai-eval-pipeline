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

{% set is_ci = target.name == 'ci' %}

{% if is_ci %}
with ground_truth as (
    select * from {{ ref('eval_ground_truth') }}
),
{% else %}
with
{% endif %}

row_level as (
    select * from {{ ref('eval_row_level') }}
),

eligibility as (
    select * from {{ ref('eligibility_check') }}
),

inference as (
    select * from {{ ref('classify_ticket') }}
),

-- ============================================================
-- RUNTIME BEHAVIORAL METRICS (no labels required)
-- These are checked by the runtime gate via evaluation_thresholds
-- ============================================================

-- Invalid output rate: fraction of outputs that failed deterministic parse check
invalid_metrics as (
    select
        run_id,
        'invalid_output_rate' as metric_name,
        round(sum(case when is_valid_output = false then 1 else 0 end)::float / nullif(count(*), 0), 4) as metric_value,
        'overall' as segment
    from row_level
    group by run_id
),

-- Judge flag rate: fraction of outputs where LLM judge flagged issues
judge_flag_metrics as (
    select
        run_id,
        'judge_flag_rate' as metric_name,
        round(sum(case when judge_flagged = true then 1 else 0 end)::float / nullif(count(*), 0), 4) as metric_value,
        'overall' as segment
    from row_level
    group by run_id
),

-- Prediction distribution shift: max absolute difference between current
-- predicted category proportions and a reference distribution.
-- Error sentinels are excluded; they are covered by invalid_output_rate.
current_distribution as (
    select
        run_id,
        lower(predicted_category) as category,
        count(*)::float / nullif(sum(count(*)) over (partition by run_id), 0) as current_pct
    from inference
    where predicted_category not in (
        {%- for s in var('error_sentinels') %}'{{ s }}'{{ ', ' if not loop.last }}{% endfor -%}
    )
    group by run_id, lower(predicted_category)
),

{% if is_ci %}
-- In CI: compare against golden dataset label distribution
reference_distribution as (
    select
        lower(category_ground_truth) as category,
        count(*)::float / nullif((select count(*) from eligibility where is_eligible), 0) as reference_pct
    from eligibility
    where is_eligible and category_ground_truth is not null
    group by lower(category_ground_truth)
),
{% else %}
-- In PROD: compare against a uniform distribution over the fixed taxonomy
-- (no labels available). A run that collapses onto one class scores 1 - 1/N.
reference_distribution as (
    {%- set categories = var('categories') %}
    {%- for c in categories %}
    select '{{ c }}' as category, 1.0 / {{ categories | length }} as reference_pct
    {%- if not loop.last %} union all{% endif %}
    {%- endfor %}
),
{% endif %}

-- Score every run against every category seen on either side, so run_id is
-- never NULL and a category that is never predicted counts as 0%.
shift_runs as (
    select distinct run_id from inference
),

shift_categories as (
    select category from reference_distribution
    union
    select category from current_distribution
),

distribution_shift as (
    select
        sr.run_id,
        'prediction_distribution_shift' as metric_name,
        round(max(abs(coalesce(c.current_pct, 0) - coalesce(r.reference_pct, 0))), 4) as metric_value,
        'overall' as segment
    from shift_runs sr
    cross join shift_categories sc
    left join current_distribution c
        on c.run_id = sr.run_id
        and c.category = sc.category
    left join reference_distribution r
        on r.category = sc.category
    group by sr.run_id
),

-- Eligible coverage: use inference run_id to stay consistent with other metrics
coverage_metrics as (
    select
        i.run_id,
        'eligible_coverage' as metric_name,
        round(sum(case when e.is_eligible then 1 else 0 end)::float / nullif(count(*), 0), 4) as metric_value,
        'overall' as segment
    from eligibility e
    cross join (select distinct run_id from inference) i
    group by i.run_id
)

{% if is_ci %}
-- ============================================================
-- OFFLINE EVALUATION ACCURACY METRICS (labels required, CI only)
-- These are checked by regression_check via baseline_metrics
-- ============================================================

, overall_metrics as (
    select
        run_id,
        'overall_accuracy' as metric_name,
        round(sum(case when is_correct then 1 else 0 end)::float / nullif(count(*), 0), 4) as metric_value,
        'overall' as segment
    from ground_truth
    group by run_id
)

, category_metrics as (
    select
        run_id,
        'category_accuracy_' || lower(expected_category) as metric_name,
        round(sum(case when is_correct then 1 else 0 end)::float / nullif(count(*), 0), 4) as metric_value,
        expected_category as segment
    from ground_truth
    group by run_id, expected_category
)
{% endif %}

-- Union all metrics
select * from invalid_metrics
union all
select * from judge_flag_metrics
union all
select * from distribution_shift
union all
select * from coverage_metrics
{% if is_ci %}
union all
select * from overall_metrics
union all
select * from category_metrics
{% endif %}
