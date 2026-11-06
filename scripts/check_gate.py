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

"""Quality gate enforcement for CI and production.

Two modes, one per database (pick the one you built):
  --mode regression  (offline evaluation)
    Queries regression_check for any REGRESSION status.
    Fails CI if a code/prompt/model change degraded metrics
    below baseline.

  --mode runtime  (online evaluation)
    Queries gate_decision for the latest run's PASS/FAIL.
    Fails CI if the pipeline produced output below quality
    thresholds.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, List

try:
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    _HAS_CONNECTOR = True
except ImportError:
    _HAS_CONNECTOR = False

_REGRESSION_QUERY = "SELECT * FROM offline.regression_check WHERE run_id = %s"
_RUN_RESULTS = Path("target/run_results.json")
_GATE_QUERY = (
    "SELECT gate_result, total_failures, critical_failures "
    "FROM online.gate_decision "
    "ORDER BY evaluated_at DESC LIMIT 1"
)


def _load_private_key(path: str) -> Any:
    """Load a PEM private key from disk.

    Args:
        path: Filesystem path to the PEM key file.

    Returns:
        A private key object for Snowflake authentication.
    """
    key_data = Path(path).read_bytes()
    private_key = serialization.load_pem_private_key(
        key_data, password=None, backend=default_backend()
    )
    return private_key


def get_connection() -> "snowflake.connector.SnowflakeConnection":
    """Create a Snowflake connection from environment variables.

    Uses key-pair auth and the same variables as profiles.yml. The role
    has no default: set SNOWFLAKE_ROLE to a least-privilege pipeline role.

    Returns:
        A snowflake.connector connection object.

    Raises:
        KeyError: If a required environment variable is not set.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        private_key=_load_private_key(
            os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
        ),
        warehouse=os.environ.get(
            "SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"
        ),
        database=os.environ.get(
            "SNOWFLAKE_DATABASE", "LLM_PIPELINE_PROD"
        ),
        schema="public",
    )


def resolve_run_id(explicit: str | None) -> str:
    """Return the dbt invocation id whose regression rows should be checked.

    Args:
        explicit: Value passed via --run-id, if any.

    Returns:
        The explicit id, else the invocation_id of the last dbt command
        recorded in target/run_results.json.

    Raises:
        ValueError: If no id is given and run_results.json is unreadable.
    """
    if explicit:
        return explicit
    try:
        metadata = json.loads(_RUN_RESULTS.read_text())["metadata"]
        return metadata["invocation_id"]
    except (OSError, KeyError, ValueError) as exc:
        raise ValueError(
            f"Could not read invocation_id from {_RUN_RESULTS}; "
            "run dbt build first or pass --run-id."
        ) from exc


def check_regression(
    conn: "snowflake.connector.SnowflakeConnection",
    run_id: str,
) -> bool:
    """Detect regressions against stored baselines.

    Args:
        conn: Active Snowflake connection.
        run_id: dbt invocation id that built regression_check; rows from
            any other run are ignored.

    Returns:
        True if every baselined metric passed, False on any regression,
        missing metric, or empty result.
    """
    print("=" * 60)
    print("OFFLINE EVALUATION: REGRESSION CHECK")
    print("=" * 60)
    print(f"  run_id: {run_id}")

    cursor = conn.cursor()
    cursor.execute(_REGRESSION_QUERY, (run_id,))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not rows:
        print(
            "  regression_check has no rows for this run_id. "
            "It was not built, or another run overwrote it."
        )
        print()
        print("RESULT: Nothing was checked. Blocking merge.")
        return False

    failures: List[dict[str, Any]] = []
    for row in rows:
        record = dict(zip(columns, row))
        status = record.get("REGRESSION_STATUS", "UNKNOWN")
        metric = record.get("METRIC_NAME", "unknown")
        current = record.get("CURRENT_VALUE", 0)
        baseline = record.get("BASELINE_VALUE", 0)
        delta = record.get("DELTA", 0)

        icon = "PASS" if status == "PASS" else status
        print(
            f"  [{icon}] {metric}: "
            f"current={current}, baseline={baseline}, "
            f"delta={delta}"
        )

        if status != "PASS":
            failures.append(record)

    print()
    if failures:
        print(
            f"RESULT: {len(failures)} FAILED CHECK(S) "
            "(regression or missing metric)"
        )
        print(
            "PR should be blocked until regressions are fixed "
            "or baseline is updated."
        )
        return False
    print("RESULT: No regressions. Change is safe to merge.")
    return True


def check_runtime(
    conn: "snowflake.connector.SnowflakeConnection",
) -> bool:
    """Check the quality gate result for the latest run.

    Args:
        conn: Active Snowflake connection.

    Returns:
        True if the gate passed, False otherwise.
    """
    print("=" * 60)
    print("ONLINE EVALUATION: QUALITY GATE CHECK")
    print("=" * 60)

    cursor = conn.cursor()
    cursor.execute(_GATE_QUERY)
    row = cursor.fetchone()

    if not row:
        print(
            "  No gate decision found. "
            "Pipeline may not have completed."
        )
        return False

    gate_result, total_failures, critical_failures = row
    print(f"  Gate result: {gate_result}")
    print(f"  Total failures: {total_failures}")
    print(f"  Critical failures: {critical_failures}")
    print()

    if gate_result == "PASS":
        print("RESULT: Gate PASSED. Output safe to publish.")
        return True
    print("RESULT: Gate FAILED. Output blocked from publication.")
    return False


def main() -> None:
    """Parse arguments and run the appropriate gate checks."""
    parser = argparse.ArgumentParser(
        description="Quality gate check"
    )
    parser.add_argument(
        "--mode",
        choices=["regression", "runtime"],
        required=True,
        help="regression: offline gate in LLM_PIPELINE_CI; "
        "runtime: online gate in LLM_PIPELINE_PROD",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be checked without connecting "
        "to Snowflake",
    )
    parser.add_argument(
        "--run-id",
        help="dbt invocation id to check in regression mode "
        "(default: read from target/run_results.json)",
    )
    args = parser.parse_args()

    if not args.dry_run and not _HAS_CONNECTOR:
        print(
            "ERROR: snowflake-connector-python is not installed, "
            "so the gate cannot be checked. Install it, or pass "
            "--dry-run to preview without connecting.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.dry_run:
        print("=" * 60)
        print("QUALITY GATE CHECK (dry-run mode)")
        print("=" * 60)
        print()
        if args.mode == "regression":
            print(
                "Would check: regression_check "
                "for REGRESSION status"
            )
            print(
                "  Compares current metrics against "
                "seeds/baseline_metrics.csv"
            )
        else:
            print(
                "Would check: gate_decision for PASS/FAIL"
            )
            print(
                "  Compares run metrics against "
                "seeds/evaluation_thresholds.csv"
            )
        print()
        print(
            "Install snowflake-connector-python and set "
            "env vars to run live."
        )
        sys.exit(0)

    run_id = ""
    if args.mode == "regression":
        try:
            run_id = resolve_run_id(args.run_id)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    conn = get_connection()
    try:
        if args.mode == "regression":
            passed = check_regression(conn, run_id)
        else:
            passed = check_runtime(conn)
    finally:
        conn.close()

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
