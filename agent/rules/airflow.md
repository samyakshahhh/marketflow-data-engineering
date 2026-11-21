# Airflow rules

Derived from the four existing DAGs (`dags/*.py`), `src/orchestration/common.py`
and the DAG tests. Existing conventions take precedence over generic Airflow
advice. Where the repository has no established pattern, this file says so —
ask before inventing one.

Reference implementations:
- API → Bronze with dynamic mapping: `dags/market_api_to_bronze.py`
- Asset-triggered transform with short-circuit + transaction: `dags/bronze_to_silver.py`
- Asset-triggered aggregate: `dags/silver_to_gold.py`
- Simple weekly reference-data load: `dags/company_profile_ingestion.py`

---

## Version and API

- Airflow **3.4** (`docker/Dockerfile`, `ARG AIRFLOW_VERSION=3.4.0`).
- Import from the Task SDK: `from airflow.sdk import dag, task, get_current_context, Asset`.
- **TaskFlow only**: `@dag`, `@task`, `@task.short_circuit`. No classic
  operators, custom operators, custom hooks or sensors exist in the project.
- Snowflake is reached through `src.warehouse.connection.snowflake_connection()`,
  which uses the provider's `SnowflakeHook` with conn id `snowflake_default`.
  Do not instantiate hooks or `SnowflakeOperator`/`SQLExecuteQueryOperator` in DAG files.

## File layout

- One DAG per file in `dags/`; file name = `dag_id` + `.py`.
- File order: module docstring → `from __future__ import annotations` →
  imports → `logger = get_logger(__name__)` → `DAG_ID = "<dag_id>"` →
  `LAYER = audit.LAYER_<X>` → `@dag` function → module-level call `<dag_id>()`.
- The module docstring starts with `### <dag_id>` and explains *why* the DAG
  is shaped the way it is; it is passed as `doc_md=__doc__`.
- DAG files contain **no SQL** and **no credentials** (enforced by
  `tests/test_dag_static.py`). Business logic lives in `src/transformations/`.

## DAG naming

- `snake_case`.
- Layer-to-layer pipelines: `<source>_to_<target>` (`market_api_to_bronze`,
  `bronze_to_silver`, `silver_to_gold`).
- Standalone subject pipelines: `<subject>_ingestion` (`company_profile_ingestion`).

## `@dag` arguments (all four DAGs use exactly this set)

```python
@dag(
    dag_id=DAG_ID,
    description="<one sentence>",
    schedule=...,                     # see Scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),      # from src.orchestration.common
    tags=[*TAGS, "<layer-or-purpose>"],
    doc_md=__doc__,
)
```

- `start_date`: fixed `datetime(2024, 1, 1)` (no `days_ago`, no dynamic dates).
- `catchup=False`, `max_active_runs=1` everywhere.
- Tags: `TAGS = ["marketflow", "snowflake"]` plus one of `bronze`, `silver`,
  `gold`, `reference-data`.

## Scheduling

- API ingestion: cron. Existing: `"0 6 * * 1-5"` (prices, weekdays),
  `"0 7 * * 1"` (profiles, weekly Monday).
- Downstream transformations: **asset-driven**, `schedule=[<UPSTREAM>_ASSET]`.
  Do not use staggered cron times to chain DAGs.
- Assets are defined only in `src/orchestration/common.py` with URI
  `snowflake://<account>/financial_market_db/<schema>/<table>`; the DAG's final
  task publishes the asset via `@task(outlets=[...])`, and only after the run
  fully succeeded.
- Adding an asset or changing a schedule needs approval (`development.md` §6).

## Retries, retry delay, failure handling

`default_args()` in `src/orchestration/common.py` — do not override per DAG
unless the Jira ticket says so:

| Key | Value |
| --- | --- |
| `owner` | `data-engineering` |
| `retries` | 3 (`tests/test_dag_integrity.py` requires ≥ 2) |
| `retry_delay` | 2 minutes |
| `retry_exponential_backoff` | True |
| `max_retry_delay` | 15 minutes |
| `on_failure_callback` | `record_task_failure` → writes FAILED row to `AUDIT.ETL_RUN_LOG` |

A new DAG must be added to `DAG_LAYERS` in `src/orchestration/common.py` so the
failure callback records the correct layer (shared file → approval required,
but this one-line registration is the established pattern).

## Notifications

**No notification mechanism exists** (no email, Slack, PagerDuty, Teams).
Failures are visible in the Airflow UI/logs and in `AUDIT.ETL_RUN_LOG`.
Failing data-quality checks are logged as warnings and persisted, and
deliberately do **not** fail the task. Do not add notifications without a
Jira requirement and an agreed channel/secret mechanism.

## Task naming and structure

- Task names: `verb_noun` snake_case, from the function name
  (`prepare_run`, `build_fetch_plan`, `ingest_symbol`, `plan_window`,
  `transform_to_silver`, `check_data_quality`, `advance_watermark`,
  `check_silver_availability`, `calculate_daily_performance`,
  `calculate_summary_metrics`, `record_run_audit`, `ingest_profiles`,
  `merge_profiles_to_silver`).
- First task is always `prepare_run`, returning
  `{"run_id": run_id_from_context(context), "batch_id": new_batch_id()}`;
  that identity dict is passed to every later task.
- "Is there work?" tasks use `@task.short_circuit` and return `{}` when there
  is nothing to do (skips the rest of the run).
- Last task records the run (`record_run_audit`) and/or advances the
  watermark (`advance_watermark`) and carries the `outlets`.
- Watermarks are advanced only after the data step succeeded.

## Inside a task

```python
@task
def do_something(identity: dict[str, str], window: dict[str, Any]) -> dict[str, Any]:
    """One line on what and why."""
    context = get_current_context()
    run_id, batch_id = identity["run_id"], identity["batch_id"]
    task_id = task_id_from_context(context)

    with snowflake_connection() as connection:
        audit.start_step(connection, run_id=run_id, dag_id=DAG_ID, task_id=task_id,
                         batch_id=batch_id, layer=LAYER)
        result = <src.transformations function>(connection, ...)
        audit.finish_step(connection, run_id=run_id, dag_id=DAG_ID, task_id=task_id,
                          status=audit.STATUS_SUCCESS, source_record_count=...,
                          target_record_count=..., rejected_record_count=...)
    return result.as_dict()
```

- Each task opens and closes its own connection.
- Multi-statement atomic work uses `with transaction(connection):` from
  `src.warehouse.loader`.
- XCom values are small JSON-serialisable dicts; domain objects cross task
  boundaries via `as_dict()` / `from_dict()` (`ProcessingWindow`, `FetchPlanItem`).
- Per-entity parallelism uses dynamic task mapping:
  `ingest_symbol.partial(identity=identity).expand(item=plan)`, with
  `max_active_tis_per_dag=2` to respect API limits.
- Dependencies: explicit `>>` chains when data flow alone does not imply order.

## Connections and configuration

- Connection id: `snowflake_default` (assembled from `.env` in
  `docker-compose.yml` as `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`). Override only via
  `SNOWFLAKE_CONN_ID` env var. No other Airflow connections exist.
- Airflow Variables are **not used**. Business config comes from
  `src.utils.config.get_config()` (YAML + env overrides).
- Symbols come from `config/symbols.yaml` (or `MARKETFLOW_SYMBOLS`), never hardcoded.

## Logging

- `logger = get_logger(__name__)` from `src.utils.logging_utils`.
- `%`-style lazy formatting, `key=value` pairs: `logger.info("... run_id=%s batch_id=%s", ...)`.
- Never log credentials; pass URLs through `redact()`.

## Registering a new DAG (checklist)

1. `dags/<dag_id>.py` following the structure above.
2. Logic in `src/transformations/<module>.py`; SQL in `sql/transformations/<layer>/`.
3. Add `"<dag_id>": audit.LAYER_<X>` to `DAG_LAYERS` in `src/orchestration/common.py`.
4. Add the file/ID to `EXPECTED_DAG_IDS` in `tests/test_dag_static.py`
   (that test asserts the **exact** set of DAG files — it fails otherwise)
   and to `EXPECTED_DAG_IDS` in `tests/test_dag_integrity.py`.
5. Add unit tests for the new `src/` logic.
6. Update README "The DAGs" and `docs/DATA_FLOW.md` §5.

## Testing

- `tests/test_dag_static.py` — runs anywhere: files parse, declare `DAG_ID`,
  use `doc_md=__doc__`, contain no credentials and no SQL.
- `tests/test_dag_integrity.py` — needs Airflow (container only): DagBag
  import errors, expected IDs, retries/owner, asset scheduling, cron/catchup.
