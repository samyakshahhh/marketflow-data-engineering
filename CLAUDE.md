# CLAUDE.md — MarketFlow development agent operating contract

This file is the first thing the agent reads in this repository. It defines what
MarketFlow is, where everything lives, and the rules the agent must follow for
every request. The repository is the persistent source of truth for project
knowledge, memory and development history.

---

## 1. What MarketFlow is

MarketFlow ingests daily stock market data (end-of-day prices and company
profiles) from the Financial Modeling Prep (FMP) REST API, lands it unchanged in
Snowflake, validates and de-duplicates it, and builds analytical datasets for BI.
It follows a medallion architecture (Bronze / Silver / Gold) with a separate
AUDIT schema, orchestrated by Apache Airflow and run locally with Docker.

Full human documentation: [`README.md`](README.md) and
[`docs/DATA_FLOW.md`](docs/DATA_FLOW.md). Do not duplicate them — link to them.

## 2. Technology stack (as found in the repository)

| Area | Technology | Evidence |
| --- | --- | --- |
| Orchestration | Apache Airflow 3.4, LocalExecutor, TaskFlow API (`airflow.sdk`) | `docker/Dockerfile`, `docker-compose.yml`, `dags/` |
| Warehouse | Snowflake, database `FINANCIAL_MARKET_DB` | `sql/01..07_*.sql`, `config/pipeline.yaml` |
| Language | Python 3.13 (Airflow image) | `requirements.txt` |
| Transformations | Snowflake SQL files (MERGE, window functions, VARIANT parsing) | `sql/transformations/` |
| API access | `requests` + urllib3 retry | `src/api/fmp_client.py` |
| Runtime | Docker / Docker Compose | `docker-compose.yml` |
| Tests | pytest | `pytest.ini`, `tests/` |
| Config | YAML for business config, environment variables for secrets | `config/`, `.env.example` |

Not present: CI/CD, linter configuration, dbt, stored procedures, Snowflake
tasks/streams, alerting/notification integrations.

## 3. Where things live

| What | Location |
| --- | --- |
| Airflow DAGs | `dags/` (one DAG per file, file name = `dag_id`) |
| Shared Airflow building blocks (assets, `default_args`, failure callback) | `src/orchestration/common.py` |
| API client | `src/api/` |
| Transformation orchestration (window planning, statement order) | `src/transformations/` |
| Snowflake connection, statement execution, audit helpers | `src/warehouse/` |
| API-response validation | `src/validation/` |
| Config and logging helpers | `src/utils/` |
| Snowflake DDL (run once, numbered) | `sql/01..07_*.sql` |
| Snowflake DML (one statement per file) | `sql/transformations/{bronze,silver,gold,audit}/` |
| Business/runtime configuration | `config/pipeline.yaml`, `config/symbols.yaml` |
| Tests | `tests/` |
| Human documentation | `README.md`, `docs/` |
| **Agent rules** | `agent/rules/` |
| **Agent memory** (project + developer preferences) | `agent/memory/` |
| **Jira context and audit trail** | `agent/jira/<JIRA-ID>/` |
| **Templates for agent artifacts** | `agent/templates/` |

## 4. Mandatory behaviour

For every request, the agent must:

1. Read this `CLAUDE.md` first.
2. Read the relevant rules in `agent/rules/` before development
   (`development.md` always; plus `airflow.md`, `snowflake.md`, `python.md`,
   `security.md`, `validation.md` as relevant).
3. Read relevant business documentation in `docs/business/`, `README.md` and
   `docs/DATA_FLOW.md`.
4. Check Jira context in `agent/jira/<JIRA-ID>/` before development.
5. Inspect existing code before creating new code.
6. Reuse existing patterns and utilities.
7. Never invent architecture.
8. Never invent business rules. If required business information is not
   found, say "not found in the repository" and ask.
9. Never hardcode secrets.
10. Never expose credentials (never read, print or copy `.env`).
11. Never modify unrelated files.
12. Validate changes before declaring development complete.
13. Maintain the Jira-specific audit in `agent/jira/<JIRA-ID>/`.
14. Keep documentation synchronized with significant architectural changes.

## 5. Jira gate (mandatory)

Any request that **creates, modifies or deletes a tracked project file**
(code, SQL, DAGs, tests, config, Docker files, scripts, documentation) requires a
Jira ID **before implementation starts**.

If no Jira ID was given, respond:

> Please provide the Jira ID before development can begin.

and do not edit anything. Questions, explanations, code review and read-only
inspection do not need a Jira ID. Full rules: `agent/jira/README.md` and
`agent/rules/development.md` §2.

## 6. Default development workflow

1. Receive the request.
2. Decide whether it changes the repository.
3. If yes, require a Jira ID (gate above).
4. Load `agent/jira/<JIRA-ID>/` (create it from `agent/templates/` if new; never overwrite existing files).
5. Load relevant rules (`agent/rules/`).
6. Load relevant business documentation (`docs/business/`, README, DATA_FLOW).
7. Load memory: `agent/memory/global.md` and `agent/memory/users/<developer>.md`.
8. Search the repository for similar implementations.
9. Inspect dependencies and reusable utilities.
10. Write a concise plan to `plan.md`.
11. List the files that will be created or modified.
12. Implement (after approval where `development.md` §6 requires it).
13. Run the available validation (`agent/rules/validation.md`).
14. Review the final `git diff`.
15. Check for unrelated changes, hardcoded credentials, naming violations,
    broken dependencies, invalid SQL/Python/DAG structure, missing docs and tests.
16. Update `changes.md`, `validation.md` and `decisions.md` in the Jira folder.
17. Report: what changed, why, files changed, validation (PASSED / FAILED /
    NOT RUN / NOT AVAILABLE), remaining risks, Jira audit location.

Details: `agent/rules/development.md`.

## 7. Priority order when instructions conflict

1. Security constraints (`agent/rules/security.md`) — cannot be overridden
2. Explicit current user requirement
3. Jira acceptance criteria
4. Existing project architecture
5. Existing project coding conventions
6. Project rules (`agent/rules/`)
7. Business documentation (`docs/business/`)
8. Developer preferences (`agent/memory/`)
9. Generic best practices

Generic best practices never override actual project conventions.

## 8. Memory

- Project-wide decisions/preferences: `agent/memory/global.md`
- Developer preferences: `agent/memory/users/<developer>.md`
- Save only what the developer explicitly asks to remember or clearly confirms
  as a persistent preference. When in doubt, ask:
  "Should I save this as a persistent development preference?"
- Never store secrets, credentials, account identifiers or personal contact data.

Details: `agent/memory/README.md`.

## 9. Git

- Inspect `git status` before major changes; never discard or overwrite
  uncommitted user work; never `reset --hard`/`checkout --` user changes.
- Review `git diff` after changes and confirm only intended files changed.
- Do not commit unless explicitly asked.

## 10. Quick commands

```bash
# unit tests (no Snowflake / API needed); requires: pip install -r requirements-dev.txt
python -m pytest

# full suite incl. Airflow DagBag tests, inside the running containers
docker compose exec -w /opt/airflow/project airflow-scheduler python -m pytest -q
```
