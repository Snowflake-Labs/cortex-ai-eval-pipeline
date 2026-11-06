# Copyright 2026 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""LLM-as-judge evaluation.

Scores each prediction using a second model from a different
family. Checks whether the classifier's reasoning references
actual input content and whether the category is consistent
with the reasoning.

All valid predictions are judged in one set-based AI_COMPLETE query
with temperature=0 and a JSON-schema response_format. Invalid outputs
(error sentinels, labels outside the taxonomy, confidence outside [0, 1])
skip the judge call to avoid wasting
tokens on invalid predictions.

This is a dbt Python model invoked by dbt, not run directly.
"""

import json
from typing import Any, Dict

from snowflake.snowpark import DataFrame, Session


JUDGE_INSTRUCTIONS = (
    "You are an AI quality auditor. Given the original input and "
    "the classifier's output, evaluate whether:\n"
    "1. The reasoning references actual content from the input "
    "(not hallucinated facts)\n"
    "2. The chosen category is consistent with the reasoning\n"
    "Flag any issues found.\n\n"
    "Score reasoning_quality (1-5: does reasoning reference actual "
    "input content?), guideline_adherence (1-5: is the category "
    "consistent with the reasoning?), overall_score (1.0-5.0), set "
    "judge_flagged to true if any issues are found, and list any "
    "problems in issues.\n\n"
)

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning_quality": {"type": "integer"},
        "guideline_adherence": {"type": "integer"},
        "overall_score": {"type": "number"},
        "judge_flagged": {"type": "boolean"},
        "issues": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "reasoning_quality",
        "guideline_adherence",
        "overall_score",
        "judge_flagged",
        "issues",
    ],
}

# SQL object literal; kept out of the f-string below because dbt scans
# Python model source for Jinja and would read doubled braces as a tag.
MODEL_PARAMETERS = "{'temperature': 0}"


def sql_str(value: str) -> str:
    """Quote a Python string as a Snowflake SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def model(dbt: Any, session: Session) -> DataFrame:
    """Score each prediction with an LLM judge.

    Joins inference results with original ticket text, judges all
    valid predictions in one AI_COMPLETE query, and returns per-row
    quality scores. Invalid outputs are scored without a judge call.

    Args:
        dbt: dbt model context object.
        session: Snowpark session.

    Returns:
        Snowpark DataFrame with one row per evaluated prediction.
    """
    dbt.config(
        materialized="table",
        packages=["snowflake-snowpark-python"],
    )
    # Set under config.meta in models/evaluation/schema.yml from
    # var('eval_llm_model') and var('categories').
    meta: Dict[str, Any] = dbt.config.get("meta")
    eval_model: str = meta["eval_llm_model"]
    allowed = ", ".join(sql_str(c) for c in meta["categories"].split(","))

    dbt.ref("classify_ticket").create_or_replace_temp_view("INFERENCE_INPUT")
    dbt.ref("eligibility_check").create_or_replace_temp_view(
        "ELIGIBILITY_INPUT"
    )

    evaluated_at = (
        "to_varchar(sysdate(), "
        "'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"+00:00\"')"
    )

    return session.sql(f"""
        with predictions as (
            select
                i.ticket_id,
                i.run_id,
                coalesce(e.ticket_text, '') as ticket_text,
                i.predicted_category,
                i.confidence,
                i.reasoning,
                -- Valid = a known label (classify_ticket already lower/trims it;
                -- error sentinels are not in the set), a confidence in [0, 1],
                -- and non-empty reasoning.
                (
                    coalesce(i.predicted_category in ({allowed}), false)
                    and coalesce(i.confidence between 0 and 1, false)
                    and coalesce(trim(i.reasoning) != '', false)
                ) as is_valid_output
            from INFERENCE_INPUT i
            join ELIGIBILITY_INPUT e on e.ticket_id = i.ticket_id
        ),

        judged as (
            select
                p.*,
                ai_complete(
                    model => {sql_str(eval_model)},
                    prompt => {sql_str(JUDGE_INSTRUCTIONS)}
                        || 'Input: ' || p.ticket_text
                        || '\\nClassification: ' || p.predicted_category
                        || '\\nConfidence: ' || p.confidence::string
                        || '\\nReasoning: ' || p.reasoning,
                    model_parameters => {MODEL_PARAMETERS},
                    response_format => parse_json({sql_str(json.dumps({"type": "json", "schema": RESPONSE_SCHEMA}))})
                ) as response
            from predictions p
            where p.is_valid_output
        )

        select
            ticket_id as TICKET_ID,
            run_id as RUN_ID,
            {sql_str(eval_model)} as JUDGE_MODEL,
            coalesce(response:overall_score::float, 0.0) as OVERALL_SCORE,
            true as IS_VALID_OUTPUT,
            -- flag if the judge says so, lists any issue, or returned nothing
            coalesce(response:judge_flagged::boolean, true)
                or coalesce(array_size(response:issues), 0) > 0 as JUDGE_FLAGGED,
            coalesce(response:reasoning_quality::int, 0) as REASONING_QUALITY,
            coalesce(response:guideline_adherence::int, 0) as GUIDELINE_ADHERENCE,
            coalesce(
                to_json(response:issues),
                '["Missing structured judge response"]'
            ) as ISSUES,
            {evaluated_at} as EVALUATED_AT
        from judged

        union all

        select
            ticket_id,
            run_id,
            {sql_str(eval_model)},
            0.0,
            false,
            true,
            0,
            0,
            '["Skipped: invalid output (unknown category, confidence outside [0, 1], or empty reasoning)"]',
            {evaluated_at}
        from predictions
        where not is_valid_output
    """)
