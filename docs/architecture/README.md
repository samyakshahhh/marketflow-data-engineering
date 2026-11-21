# Architecture

## Pipeline architecture

Documented authoritatively in:

- [`../../README.md`](../../README.md) — sections *Architecture*, *Data flow*,
  *Snowflake architecture*, *The DAGs*, *Incremental loading*, *Idempotency*,
  *Design decisions*
- [`../DATA_FLOW.md`](../DATA_FLOW.md) — §2 *End-to-End Architecture*,
  §15 *Docker Architecture*, §16 *Configuration and Secrets*

In one line: FMP API → Python client → Airflow (TaskFlow, asset-driven) →
Snowflake BRONZE (raw VARIANT, append-only) → SILVER (typed, validated,
de-duplicated via MERGE) → GOLD (window-function analytics) → BI view, with
AUDIT holding run log, data-quality results, quarantine and watermarks.

## Agent framework

- [`agent_framework.md`](agent_framework.md) — how `CLAUDE.md`, `agent/` and
  `docs/` work together
- [`future_integrations.md`](future_integrations.md) — where Jira, Git,
  Snowflake, Airflow and CI/CD tooling would plug in later

## Documentation drift observed (flagged, not changed)

Found during the initial repository inspection on 2026-11-21. Fix under a Jira ticket.

| Location | Observation |
| --- | --- |
| `src/orchestration/common.py` module docstring | Says `dags/` contains "the three pipeline definitions"; there are four DAG files (incl. `company_profile_ingestion`). |
| `NEXT_STEPS.local.md` (git-ignored local note) | States nothing is committed and Docker/WSL is blocked; the repository has a commit and `logs/` shows DAG runs from 2026-11-16/17. |
| `README.md` *License* | Refers to a `LICENSE` file that does not exist. |
| `README.md` *Project layout* | Does not list `CLAUDE.md`, `agent/` or the new `docs/` subfolders. |
