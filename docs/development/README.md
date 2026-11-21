# Developing with the MarketFlow agent

The agent follows the root [`CLAUDE.md`](../../CLAUDE.md). This page explains
what that means for a developer.

## 1. Always have a Jira ID ready

Any change to the repository needs one. Without it the agent answers:

> Please provide the Jira ID before development can begin.

Questions, explanations and reviews do not need one.

## 2. What the agent does with it

1. Opens `agent/jira/<JIRA-ID>/` — continues from it if it exists, creates it
   from `agent/templates/` if not.
2. Reads the rules (`agent/rules/`), business docs (`docs/business/`, README,
   DATA_FLOW) and memory (`agent/memory/global.md`, `agent/memory/users/<you>.md`).
3. Searches the repository for existing implementations and reports what it found.
4. Writes `plan.md` listing every file to create/modify, and waits for approval
   where `agent/rules/development.md` §6 requires it.
5. Implements, validates (PASSED / FAILED / NOT RUN / NOT AVAILABLE), reviews
   the diff, records `changes.md` / `validation.md` / `decisions.md`, and reports.

It does not commit unless you ask.

## 3. Preferences

Say "remember that …" (or confirm when the agent asks *"Should I save this as a
persistent development preference?"*) and it is stored in
`agent/memory/users/<your-git-name>.md`. Team-wide decisions go to
`agent/memory/global.md`.

## 4. Running checks yourself

```bash
pip install -r requirements-dev.txt
python -m pytest                                   # unit + static tests
docker compose exec -w /opt/airflow/project airflow-scheduler python -m pytest -q   # + DagBag tests
```

---

## Worked example: "Create a DAG for company profile ingestion."

### Turn 1 — no Jira ID

> **Developer:** Create a DAG for company profile ingestion.
>
> **Agent:** Please provide the Jira ID before development can begin.

Nothing is written to the repository.

### Turn 2 — Jira ID supplied

> **Developer:** MF-142

The agent:

1. Checks `agent/jira/MF-142/` — does not exist → creates `context.md` and
   `requirements.md` from the templates, recording the request verbatim.
2. Reads `agent/rules/development.md`, `airflow.md`, `snowflake.md`,
   `python.md`, `security.md`; `docs/business/data_definitions.md`;
   `agent/memory/global.md`.
3. **Read-before-write** search of `dags/`, `src/transformations/`, `sql/`,
   `tests/` finds that this already exists:
   - `dags/company_profile_ingestion.py` (weekly, `0 7 * * 1`)
   - `src/transformations/api_to_bronze.py::ingest_company_profile`
   - `src/transformations/bronze_to_silver.py::transform_company_profile`
   - `sql/transformations/bronze/insert_company_profile_raw.sql`,
     `silver/15_merge_company_profile.sql`, `silver/16_pending_bronze_company_profile.sql`
   - `BRONZE.COMPANY_PROFILE_RAW`, `SILVER.COMPANY_PROFILE`, `SILVER.VW_COMPANY_PROFILE_STAGING`
4. Does **not** create a duplicate DAG. Records in `decisions.md`:
   *D1 — Existing implementation found; no new DAG created pending clarification.*
5. Replies:

> **Agent:** A company profile ingestion DAG already exists:
> `dags/company_profile_ingestion.py` (API → `BRONZE.COMPANY_PROFILE_RAW` →
> `SILVER.COMPANY_PROFILE`, weekly on Mondays 07:00 UTC). Creating a second one
> would duplicate it. What should MF-142 change? For example:
> - a different schedule or symbol set,
> - data-quality checks / quarantine for profiles (today profiles are merged
>   without validation rules — prices have them, profiles do not),
> - additional profile fields,
> - something else described in the ticket.
>
> I've opened `agent/jira/MF-142/` with the request and this open question.

### Turn 3 — requirement clarified (illustrative)

> **Developer:** Add data-quality checks for company profiles: SYMBOL and
> COMPANY_NAME must be present; quarantine failures like we do for prices.

The agent writes `plan.md`, e.g.:

| Action | Path | Purpose |
| --- | --- | --- |
| modify | `sql/07_create_views.sql` | add `HAS_*` flags, `ERRORS`, `IS_VALID` to `SILVER.VW_COMPANY_PROFILE_STAGING` |
| create | `sql/transformations/silver/18_quarantine_invalid_company_profile.sql` | insert failures into `AUDIT.DATA_QUALITY_ERRORS` |
| create | `sql/transformations/silver/19_record_company_profile_dq_results.sql` | write `AUDIT.DATA_QUALITY_RESULTS` rows |
| modify | `sql/transformations/silver/15_merge_company_profile.sql` | merge only `IS_VALID` rows |
| modify | `src/transformations/bronze_to_silver.py` | run the new statements in `transform_company_profile` (in a transaction) |
| modify | `tests/test_transformations.py` | contract tests for the new SQL |
| modify | `docs/business/data_definitions.md`, `docs/DATA_FLOW.md` §7.7/§8, README *Data quality* | document the new rules |

It flags for approval: the view change (`CREATE OR REPLACE VIEW` must be re-run
in Snowflake) and the new error type name, if one is needed (e.g. reuse
`NULL_REQUIRED_FIELD`). After approval it implements, runs
`python -m pytest`, reports DagBag tests as NOT RUN if Docker is down and
Snowflake execution as NOT RUN, reviews the diff and secret scan, then appends:

- `agent/jira/MF-142/changes.md` — Change 1 with files, objects, DAGs affected
- `agent/jira/MF-142/validation.md` — per-check status table
- `agent/jira/MF-142/context.md` — status `READY FOR REVIEW`, outstanding:
  "re-run `sql/07_create_views.sql` in Snowflake"

Final report to the developer: what changed, why, files, validation statuses,
remaining risks, and the path `agent/jira/MF-142/`.
