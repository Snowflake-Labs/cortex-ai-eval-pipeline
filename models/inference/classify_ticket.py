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

"""Cortex AI ticket classifier.

Classifies all eligible support tickets in one set-based AI_COMPLETE
query. temperature=0 reduces run-to-run variation (it does not remove
it: borderline tickets can still flip), and a JSON-schema
response_format returns typed fields, so no response parsing is needed.

This is a dbt Python model invoked by dbt, not run directly.
"""

import json
from typing import Any, Dict, List

from snowflake.snowpark import DataFrame, Session


PROMPT_TEMPLATE = (
    "You are a support ticket classifier. Given a customer support "
    "ticket, classify it into exactly one of these categories:\n\n"
    "- billing: Payment issues, invoice disputes, charges, refunds, "
    "pricing questions\n"
    "- account_access: Login problems, password resets, permissions, "
    "user management\n"
    "- technical_issue: Bugs, errors, outages, performance problems, "
    "data issues\n"
    "- product_question: How-to questions, feature inquiries, "
    "documentation requests\n"
    "- cancellation: Subscription cancellation, downgrade, contract "
    "termination\n"
    "- other: Anything that does not clearly fit the above "
    "categories\n\n"
    "Return the category, a confidence from 0.0 to 1.0, and a brief "
    "explanation of why this category was chosen.\n\n"
    "Ticket text:\n"
)

# SQL object literal; kept out of the f-string below because dbt scans
# Python model source for Jinja and would read doubled braces as a tag.
MODEL_PARAMETERS = "{'temperature': 0}"


def response_schema(categories: List[str]) -> Dict[str, Any]:
    """Structured-output schema; the enum limits labels to the taxonomy."""
    return {
        "type": "object",
        "properties": {
            "predicted_category": {"type": "string", "enum": categories},
            "confidence": {"type": "number"},
            "reasoning": {"type": "string"},
        },
        "required": ["predicted_category", "confidence", "reasoning"],
    }


def sql_str(value: str) -> str:
    """Quote a Python string as a Snowflake SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def model(dbt: Any, session: Session) -> DataFrame:
    """Classify eligible tickets via Snowflake Cortex AI.

    Reads from eligibility_check and classifies every eligible ticket
    in a single AI_COMPLETE query.

    Args:
        dbt: dbt model context object.
        session: Snowpark session.

    Returns:
        Snowpark DataFrame with one row per classified ticket.
    """
    dbt.config(
        materialized="table",
        packages=["snowflake-snowpark-python"],
    )

    # Set under config.meta in models/inference/schema.yml from
    # var('llm_model'), var('prompt_version') and var('categories'), so
    # --vars overrides change the model called.
    meta: Dict[str, Any] = dbt.config.get("meta")
    llm_model: str = meta["llm_model"]
    prompt_version: str = meta["prompt_version"]
    categories: List[str] = meta["categories"].split(",")
    schema_json = json.dumps(
        {"type": "json", "schema": response_schema(categories)}
    )

    dbt.ref("eligibility_check").create_or_replace_temp_view(
        "ELIGIBILITY_INPUT"
    )

    return session.sql(f"""
        with classified as (
            select
                ticket_id,
                coalesce(ticket_text, '') as ticket_text,
                run_id,
                ai_complete(
                    model => {sql_str(llm_model)},
                    prompt => {sql_str(PROMPT_TEMPLATE)} || coalesce(ticket_text, ''),
                    model_parameters => {MODEL_PARAMETERS},
                    response_format => parse_json({sql_str(schema_json)})
                ) as response
            from ELIGIBILITY_INPUT
            where is_eligible
        )
        select
            ticket_id as TICKET_ID,
            -- normalised once here so every downstream check compares like with like;
            -- a response missing any required field is a parse error, not a valid label
            case
                when response:confidence is null or response:reasoning is null
                    then 'PARSE_ERROR'
                else coalesce(lower(trim(response:predicted_category::string)), 'PARSE_ERROR')
            end as PREDICTED_CATEGORY,
            coalesce(response:confidence::float, 0.0) as CONFIDENCE,
            coalesce(response:reasoning::string, 'Missing structured response') as REASONING,
            {sql_str(llm_model)} as MODEL_NAME,
            {sql_str(prompt_version)} as PROMPT_VERSION,
            sha2(
                ticket_id || ':' || ticket_text || ':'
                || {sql_str(prompt_version)} || ':' || {sql_str(llm_model)},
                256
            ) as INPUT_FINGERPRINT,
            run_id as RUN_ID,
            to_varchar(sysdate(), 'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"') as INFERENCE_TIMESTAMP
        from classified
    """)
