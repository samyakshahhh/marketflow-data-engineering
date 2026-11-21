# Development rules

Applies to every request that changes the repository. Companion rules:
`airflow.md`, `snowflake.md`, `python.md`, `security.md`, `validation.md`.

---

## 1. Classify the request first

| Request type | Examples | Jira ID needed |
| --- | --- | --- |
| Question / explanation | "How does the Silver watermark work?" | No |
| Read-only inspection / review | "Review `13_merge_stock_prices.sql`" | No |
| Saving a preference the developer explicitly asked to save | "Remember that I prefer…" | No (writes only to `agent/memory/`) |
| Any create / modify / delete of a tracked project file | DAGs, `src/`, `sql/`, `tests/`, `config/`, `docker/`, `docker-compose.yml`, `requirements*.txt`, `.env.example`, `scripts/`, `README.md`, `docs/`, `agent/rules/`, `agent/templates/` | **Yes** |

When unsure, treat the request as development and ask for the Jira ID.

## 2. Jira gate

1. No Jira ID → reply "Please provide the Jira ID before development can begin."
   and stop. Do not create files, do not "prepare a draft" in the repository.
   Read-only investigation to answer questions is still allowed.
2. Accept IDs matching `^[A-Z][A-Z0-9]+-[0-9]+$` (e.g. `MF-142`). If the
   developer gives something else, ask them to confirm it.
   The project's Jira key is not recorded in the repository yet; once a
   developer confirms it, save it in `agent/memory/global.md`.
3. Folder: `agent/jira/<JIRA-ID>/`.
   - Exists → read every file in it first and continue from it.
   - Missing → create it from `agent/templates/` (see `agent/jira/README.md`).
4. Never delete or rewrite earlier entries in a Jira folder. Append dated
   entries; mark superseded items as superseded instead of removing them.
5. One Jira ID per change set. If the developer switches tickets mid-session,
   confirm which ID subsequent changes belong to.

## 3. Read before write

Before creating anything new, search for an existing implementation and say
what you found (or that nothing similar exists).

**Before a new or changed Airflow DAG**, inspect:
- all files in `dags/`
- `src/orchestration/common.py` (assets, `default_args()`, `DAG_LAYERS`, failure callback)
- the `src/transformations/` module the DAG would call
- `src/warehouse/audit.py` (run log and watermark helpers)
- `config/pipeline.yaml`, `config/symbols.yaml`
- `tests/test_dag_static.py`, `tests/test_dag_integrity.py`

**Before Snowflake SQL**, inspect:
- `sql/01..07_*.sql` for existing schemas, tables, views, column types, comments
- `sql/transformations/<layer>/` for similar statements and the numbering in use
- `sql/07_create_views.sql` for staging views that already hold validation rules
- `sql/06_create_audit_tables.sql` and `sql/transformations/audit/` for audit, quarantine and watermark patterns
- `tests/test_transformations.py` for the SQL contract tests

**Before Python**, inspect `src/` for an existing helper (config, logging,
connection, statement execution, audit, validation) and reuse it.

Prefer extending an existing module over adding a parallel one.

## 4. Plan

Write `agent/jira/<JIRA-ID>/plan.md` (template: `agent/templates/development_plan_template.md`)
before implementing anything non-trivial. It must list every file to be
created, modified or deleted, and the Snowflake objects and DAGs affected.
Show the plan to the developer.

## 5. Implement

- Follow `airflow.md`, `snowflake.md`, `python.md` and `security.md`.
- Touch only the files named in the plan. If another file turns out to need a
  change, say so and update the plan.
- Do not refactor, rename or reformat unrelated code.
- Do not delete code without a stated reason recorded in `decisions.md`.

## 6. Change control — approval required first

Stop, explain and wait for explicit approval before:

- adding a new Snowflake schema, a new layer, or changing a table's business key
- altering or dropping an existing table/column (the DDL scripts are
  `CREATE ... IF NOT EXISTS`; there is no migration tool, so changes to
  existing tables need a separate, explicitly approved `ALTER` script)
- changing `src/orchestration/common.py`, `src/warehouse/`, `default_args()`,
  asset URIs or the watermark/audit mechanism (shared by every DAG)
- introducing a new dependency, service, Docker component, framework or tool
  (e.g. dbt, stored procedures, Snowflake tasks, new Airflow providers)
- changing scheduling of an existing DAG
- anything that rewrites Bronze data (Bronze is append-only)

For each: explain the proposed change, list affected components, record the
decision in `agent/jira/<JIRA-ID>/decisions.md`, then implement only after approval.

## 7. Validate and review

- Run the checks in `validation.md`; report each as PASSED / FAILED / NOT RUN / NOT AVAILABLE.
- Run `git status` and `git diff`; confirm every changed file is in the plan.
- Scan the diff for credentials, unbound `%` in SQL, SQL inside DAG files,
  naming violations, missing tests, missing docs.

## 8. Record

Update in `agent/jira/<JIRA-ID>/`:
- `changes.md` — append a dated entry (template: `changes_template.md`)
- `validation.md` — append the validation results
- `decisions.md` — any decision taken or assumption made
- `context.md` — status and outstanding items

## 9. Report to the developer

What changed, why, files changed, validation results (with status words),
remaining risks/outstanding items, and the Jira audit location.

## 10. Documentation sync

If the change alters architecture, a DAG's task graph or schedule, a Snowflake
object, a data-quality rule, configuration keys or setup steps, update the
affected sections of `README.md` / `docs/DATA_FLOW.md` / `docs/` in the same
Jira change. If documentation looks outdated but is outside the ticket's scope,
flag it in the report instead of silently rewriting it.

## 11. Git

- `git status` before starting; do not discard or overwrite uncommitted work.
- Never run `git reset --hard`, `git checkout -- <file>`, `git clean`, or
  force-push unless the developer explicitly asks.
- Do not commit unless explicitly asked. If asked, reference the Jira ID in the
  commit message (e.g. `MF-142: add …`).
