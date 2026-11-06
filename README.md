# cortex-ai-eval-pipeline

A reference implementation of **offline and online evaluation** for production AI systems, built as a dbt pipeline on Snowflake Cortex AI.

## The problem

A successful AI inference does not mean the output is safe to consume. Production AI has three distinct quality questions:

1. **Offline evaluation**: Is this code/prompt/model change safe to deploy?
2. **Online evaluation**: Is this production batch safe to publish to downstream consumers?
3. **Monitoring**: Is accuracy drifting on real production data over time?

These are different questions, answered at different times, by different mechanisms. This project implements all three in a single dbt pipeline, making evaluation mandatory by construction.

## Architecture

```
OFFLINE EVALUATION (CI/CD)
  Golden dataset (fixed, labeled)
    --> Full pipeline + LLM judge
    --> Accuracy vs stored baseline
    --> Blocks PR merge if regression detected

ONLINE EVALUATION (Every production run)
  Input Quality:
    Source --> Profiling --> Eligibility --> Inference
  Output Quality:
    Format Check + LLM Judge --> Aggregate Metrics --> Quality Gate
                                                         |
                                                    Publication (if PASS)

MONITORING (Feedback loop)
  Delayed labels arrive --> Retroactive accuracy computed
    --> Review accuracy trend (manual) --> Update golden dataset --> Re-enter offline evaluation
```

Online evaluation uses behavioral metrics (no labels needed). Offline evaluation uses accuracy metrics (labels from golden dataset). Monitoring bridges the gap with delayed labels.

## Prerequisites

- Snowflake account with [Cortex AI](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions) access
- Python 3.10+ (tested on 3.12)
- dbt-core >= 1.10.5, dbt-snowflake >= 1.10 (tested on 1.12, pinned in `requirements.txt`)
- Cortex models: `mistral-large2` (classifier), `claude-sonnet-4-5` (judge)

## Setup

```bash
git clone https://github.com/Snowflake-Labs/cortex-ai-eval-pipeline.git
cd cortex-ai-eval-pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dbt deps
```

Configure your Snowflake connection:

```bash
cp .env.example .env
# Edit .env with your Snowflake account details
```

| Variable | Required | Default |
|----------|----------|---------|
| `SNOWFLAKE_ACCOUNT` | Yes | -- |
| `SNOWFLAKE_USER` | Yes | -- |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Yes | -- |
| `SNOWFLAKE_ROLE` | Yes | -- (use a least-privilege role, see below) |
| `SNOWFLAKE_WAREHOUSE` | No | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | No | `LLM_PIPELINE_PROD` |

> Recent dbt versions read `.env` from the project directory, but older ones and `scripts/check_gate.py` do not. Source it before running:
> ```bash
> source .env && export SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PRIVATE_KEY_PATH SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE
> ```

Authentication is key-pair only, for both dbt and `scripts/check_gate.py`.

### Least-privilege role

Run the pipeline with a dedicated role rather than `SYSADMIN` ([access control considerations](https://docs.snowflake.com/en/user-guide/security-access-control-considerations)). It needs to own the two pipeline databases, use one warehouse, and call `AI_COMPLETE`, which requires the `USE AI FUNCTIONS` privilege plus the `CORTEX_USER` (or `AI_FUNCTIONS_USER`) database role ([Cortex privileges](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-privileges-and-access)). Both are granted to `PUBLIC` by default; grant them explicitly if your account has revoked that.

```sql
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS LLM_PIPELINE_ROLE;
CREATE DATABASE IF NOT EXISTS LLM_PIPELINE_CI;
CREATE DATABASE IF NOT EXISTS LLM_PIPELINE_PROD;
GRANT OWNERSHIP ON DATABASE LLM_PIPELINE_CI TO ROLE LLM_PIPELINE_ROLE;
GRANT OWNERSHIP ON DATABASE LLM_PIPELINE_PROD TO ROLE LLM_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LLM_PIPELINE_ROLE;
GRANT USE AI FUNCTIONS ON ACCOUNT TO ROLE LLM_PIPELINE_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE LLM_PIPELINE_ROLE;
GRANT ROLE LLM_PIPELINE_ROLE TO USER <your_user>;
```

## Seed data

All input data is stored as dbt seed CSVs under `seeds/`. Each seed is tagged so the correct subset loads per environment.

| Seed | Used in | Description |
|------|---------|-------------|
| `golden_tickets.csv` | Offline | 40 labeled tickets with known-correct categories. Used as ground truth for regression testing. |
| `baseline_metrics.csv` | Offline | 3 accuracy baselines. Offline evaluation flags regressions against these. |
| `production_tickets.csv` | Online | 20 unlabeled tickets for live classification. |
| `evaluation_thresholds.csv` | Online | 4 quality gate thresholds (e.g., max `invalid_output_rate`, max `judge_flag_rate`). |
| `delayed_labels.csv` | Monitoring | 17 human labels that arrive after predictions are published. Joined to compute retroactive accuracy. |

## Run: offline evaluation

**Purpose**: Is this prompt/model change safe to deploy?

This runs as part of the CI workflow (`.github/workflows/ci.yml`):

```bash
DBT_PROFILES_DIR=. dbt build --target ci --full-refresh --exclude tag:runtime tag:monitoring
```

`--target ci` points to the `LLM_PIPELINE_CI` database and swaps the source to `golden_tickets` (40 labeled tickets). `--exclude tag:runtime tag:monitoring` strips out production-only models (quality gate, publication, run history) and deferred monitoring (retroactive accuracy).

> All PRs share the `LLM_PIPELINE_CI` database, so CI runs the Snowflake job one PR at a time (a global concurrency group), and the regression gate only reads rows from its own dbt run. GitHub keeps only the newest waiting run in a group, so if several PRs queue at once an older waiting run is cancelled and needs a re-run.

> Pull requests from forks get no repository secrets, so they run lint and `dbt parse` only; the Snowflake gate runs on PRs from branches in this repository.

This materializes the following tables in `LLM_PIPELINE_CI`:

1. **Eligibility** — `PIPELINE.ELIGIBILITY_CHECK` rejects invalid records before they reach inference (empty text, too short, unsupported language)
2. **Inference** — `PIPELINE.CLASSIFY_TICKET` classifies each eligible ticket via Cortex AI (`mistral-large2`)
3. **Output validation + LLM judge** — `EVALUATION.EVAL_ROW_LEVEL` runs a deterministic format check and a `claude-sonnet-4-5` judge that scores reasoning quality per row
4. **Aggregate metrics** — `EVALUATION.EVAL_AGGREGATE` rolls up row-level results into `invalid_output_rate`, `judge_flag_rate`, `prediction_distribution_shift`, plus CI-only accuracy metrics: `overall_accuracy` and per-category `category_accuracy_{category}`
5. **Ground truth comparison** — `OFFLINE.EVAL_GROUND_TRUTH` joins predictions against labeled ground truth and computes `is_correct`
6. **Regression check** — `OFFLINE.REGRESSION_CHECK` compares current accuracy against baselines configured in `seeds/baseline_metrics.csv`; status is `PASS` or `REGRESSION`

## Run: online evaluation

**Purpose**: Is this production batch safe to publish?

```bash
DBT_PROFILES_DIR=. dbt build --exclude tag:change_time tag:monitoring
```

`--exclude tag:change_time tag:monitoring` strips out CI-only models (ground truth comparison, regression check) and deferred monitoring (retroactive accuracy).

This materializes the following objects in `LLM_PIPELINE_PROD`:

1. **Staging** — `PIPELINE.STG_SUPPORT_TICKETS` (view, reads `production_tickets`)
2. **Profiling** — `PIPELINE.PROFILE_INPUT_HEALTH` (dataset health snapshot: null rates, text lengths, volume)
3. **Eligibility** — `PIPELINE.ELIGIBILITY_CHECK` (rejects empty text, too-short text, unsupported languages)
4. **Inference** — `PIPELINE.CLASSIFY_TICKET` (Cortex AI classification via `mistral-large2`)
5. **Output validation + LLM judge** — `EVALUATION.EVAL_ROW_LEVEL` (deterministic format check + `claude-sonnet-4-5` judge scoring)
6. **Aggregate metrics** — `EVALUATION.EVAL_AGGREGATE` (rolls up `invalid_output_rate`, `judge_flag_rate`, `prediction_distribution_shift`, `eligible_coverage`)
7. **Quality gate** — `ONLINE.GATE_DECISION` compares metrics against thresholds configured in `seeds/evaluation_thresholds.csv`; produces `PASS` or `FAIL`
8. **Publication** — `ONLINE.PUBLISHED_PREDICTIONS` (incremental, appends predictions from passing runs only; a failing run publishes nothing and keeps earlier batches) and `ONLINE.CURRENT_PREDICTIONS` (view, latest prediction per ticket)
9. **Run history** — `MONITORING.RUN_HISTORY` (incremental, appends one summary row per run)

## Run: monitoring

**Purpose**: Is accuracy drifting on real production data over time?

Neither evaluation catches slow accuracy drift on live data — online evaluation has no labels, and offline evaluation only runs on PR changes. Monitoring closes this gap by joining predictions against human labels that arrive days or weeks later.

Update `seeds/delayed_labels.csv` with new human labels, then run:

```bash
DBT_PROFILES_DIR=. dbt build --select tag:monitoring
```

This materializes the following table in `LLM_PIPELINE_PROD`:

1. **Retroactive accuracy** — `MONITORING.RETROACTIVE_ACCURACY` joins every published batch against `delayed_labels` and reports `retroactive_accuracy` per `run_id` / `model_name` / `prompt_version`

If accuracy drops, add misclassified examples to `seeds/golden_tickets.csv` and push through offline evaluation to recalibrate.

## Configuration

Key variables in `dbt_project.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `llm_model` | `mistral-large2` | Model for classification |
| `eval_llm_model` | `claude-sonnet-4-5` | Model for LLM judge |
| `prompt_version` | `v1.0` | Tracked in input fingerprint |
| `min_text_length` | `20` | Eligibility threshold |
| `categories` | 6-category taxonomy | Classes scored by `prediction_distribution_shift`; the PROD reference is uniform over these |
| `error_sentinels` | `PARSE_ERROR`, `LLM_ERROR` | Inference failure values excluded from the distribution (covered by `invalid_output_rate`) |

## Known simplifications (by design)

This repo is a reference implementation, so some production concerns are deliberately kept simple:

- **Distribution shift uses a uniform reference.** Real ticket mixes aren't uniform; in production you'd compare against a stored historical distribution.
- **Shift is a non-critical warning.** It reports drift and class collapse but doesn't block publication. Make it critical only once the reference reflects real traffic.
- **Batches are small, so per-run metrics are noisy.** Tune thresholds in `seeds/evaluation_thresholds.csv` against your own volumes.

## Reference

### Databases

| Database | Purpose | Build command |
|----------|---------|---------------|
| `LLM_PIPELINE_CI` | Offline evaluation (CI) | `dbt build --target ci --exclude tag:runtime tag:monitoring` |
| `LLM_PIPELINE_PROD` | Online evaluation (production) | `dbt build --exclude tag:change_time tag:monitoring` |

### Schemas

| Schema | Models | Purpose |
|--------|--------|---------|
| `PIPELINE` | stg_support_tickets, profile_input_health, eligibility_check, classify_ticket | Input through inference |
| `EVALUATION` | eval_row_level, eval_aggregate | LLM judge + aggregate metrics |
| `OFFLINE` | eval_ground_truth, regression_check | Accuracy vs baseline (CI only) |
| `ONLINE` | gate_decision, published_predictions, current_predictions | Quality gate + publication (prod only) |
| `MONITORING` | run_history, retroactive_accuracy | Run tracking + delayed-label accuracy (prod only) |
| `SEEDS` | golden_tickets, production_tickets, delayed_labels, evaluation_thresholds, baseline_metrics | Reference data |

### Project structure

```
models/
  staging/          --> PIPELINE           Source data normalization
  profiling/        --> PIPELINE           Dataset-level health metrics
  eligibility/      --> PIPELINE           Per-record pass/fail with reason
  inference/        --> PIPELINE           Cortex AI classification
  evaluation/       --> EVALUATION         LLM judge + format check + metrics
  quality_gate/     --> ONLINE              Quality gate PASS/FAIL decision
  publication/      --> ONLINE              Only from passing runs
  monitoring/       --> MONITORING         Run history + retroactive accuracy
  change_time/      --> OFFLINE             Regression detection vs baseline
seeds/
  golden_tickets.csv          40 labeled tickets (CI)
  production_tickets.csv      20 unlabeled tickets (prod)
  delayed_labels.csv          Human labels for monitoring
  evaluation_thresholds.csv   Online evaluation thresholds
  baseline_metrics.csv        Offline evaluation baselines
scripts/
  check_gate.py               CI enforcement script
```

## License

Apache 2.0

