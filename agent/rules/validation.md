# Validation rules

Validate before declaring any development complete, using only tooling that
actually exists in the repository. Never claim a check passed unless it was
executed (or otherwise verified) in this session.

## Status vocabulary (use exactly these words)

| Status | Meaning |
| --- | --- |
| **PASSED** | Executed in this session and succeeded |
| **FAILED** | Executed and failed — include the relevant output |
| **NOT RUN** | Possible in principle but not executed (say why, e.g. "Docker not running") |
| **NOT AVAILABLE** | The repository has no tooling for this check |

## Available tooling

| Check | Command | Needs |
| --- | --- | --- |
| Unit + static tests | `python -m pytest` | `pip install -r requirements-dev.txt` (requests, PyYAML, pytest) |
| Targeted tests | `python -m pytest tests/test_transformations.py -q` | same |
| Full suite incl. DagBag import | `docker compose exec -w /opt/airflow/project airflow-scheduler python -m pytest -q` | Docker stack running |
| DAG list / import errors | `docker compose run --rm airflow-cli airflow dags list-import-errors` | Docker |
| Python syntax | `python -m py_compile <file>` | Python |
| Snowflake DDL dry run | `python scripts/setup_snowflake.py --dry-run` (lists scripts only) | Python |
| Integration tests | `python -m pytest -m integration` | live credentials — run only if the developer asks |
| Linting | — | **NOT AVAILABLE** (no linter configured) |
| CI pipeline | — | **NOT AVAILABLE** (no CI configured) |
| SQL execution against Snowflake | — | **NOT RUN** by default: needs credentials and touches a real warehouse; only on explicit developer request |

## What to validate per change type

**Python**
- `py_compile` on changed files; pytest for the affected area.
- New logic has unit tests using `FakeConnection` / stubs.

**Airflow DAG**
- `tests/test_dag_static.py` (always runnable once pytest is installed).
- `tests/test_dag_integrity.py` inside the container (otherwise NOT RUN — say so).
- DAG registered in `DAG_LAYERS` and both `EXPECTED_DAG_IDS` sets.
- Dependencies between tasks reviewed against the intended graph.

**Snowflake SQL**
- `tests/test_transformations.py` SQL contract tests (bind params, no credentials,
  every referenced path exists).
- Manual review: object names exist in `sql/01..07` (or are created by this change),
  MERGE keys match the documented business key, lineage columns populated,
  no literal `%`, one statement per transformation file.
- Actual execution in Snowflake: NOT RUN unless the developer requests it.

**Configuration**
- YAML parses (`test_config.py`); keys referenced in `src/utils/config.py` exist.
- No secrets in config; new secrets only as names in `.env.example`.

**Every change**
- `git status` / `git diff` — only planned files changed.
- Secret scan from `security.md`.
- Documentation updated where `development.md` §10 requires it.

Record results in `agent/jira/<JIRA-ID>/validation.md` using
`agent/templates/validation_template.md`.
