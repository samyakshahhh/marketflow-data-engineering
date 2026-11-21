# Security rules

These rules have the highest priority (`CLAUDE.md` §7). No user preference,
memory entry or Jira requirement overrides them.

---

## Never

- hardcode passwords, API keys, tokens, private keys, Fernet keys or JWT secrets
- expose secrets in code, SQL, YAML, logs, exceptions, XCom, Snowflake tables,
  test fixtures, documentation, commit messages, `agent/memory/` or `agent/jira/`
- read, print, `cat`, copy or summarise `.env` (or any `.env.*` other than `.env.example`)
- commit credentials or files matching `.gitignore` secret patterns (`.env`, `*.pem`, `*.p8`)
- log a full request URL without `redact()` (the FMP key travels as `apikey=` query param)
- put real values in `.env.example` — it holds variable names and safe defaults only
- create insecure credential handling (secrets as CLI args, in Airflow Variables
  in plain text, in DAG default args, in asset URIs, in `API_ENDPOINT` columns)
- weaken an existing protection (e.g. remove a credential test, broaden a log line)

## Approved secret mechanism (the only one in the repository)

| Secret | Source | Consumed by |
| --- | --- | --- |
| `FMP_API_KEY` | env var from `.env` | `src/api/fmp_client.py` (`os.getenv`) |
| `SNOWFLAKE_ACCOUNT/USER/PASSWORD/ROLE/WAREHOUSE/DATABASE/SCHEMA` | env vars from `.env` | `docker-compose.yml` builds `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`; `src/warehouse/connection.py` falls back to env vars outside Airflow |
| `FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, UI user/password | env vars from `.env` | `docker-compose.yml` |

`.env` is created by copying `.env.example` and is git-ignored.

A new secret: add the variable **name** (empty value) to `.env.example` with a
comment, read it with `os.getenv` in the one module that needs it, fail fast
with a clear message when missing, and never log it.

If a requirement needs a different mechanism (secrets manager, key-pair auth,
Airflow secrets backend), **ask** — do not invent one.

## Known local-development defaults (do not copy into anything new)

`docker-compose.yml` and `.env.example` contain local-only defaults: Postgres
`airflow/airflow`, Airflow UI `airflow/airflow`, and a default JWT secret.
They are acceptable for the local Docker stack only. Do not reuse them, and
flag them if a ticket moves toward a shared or deployed environment.

## Existing automated protections (keep them passing)

- `tests/test_dag_static.py::test_dag_files_contain_no_credentials`
- `tests/test_transformations.py::test_sql_contains_no_credentials`
- `tests/test_api_client.py::test_transport_errors_are_wrapped_and_do_not_leak_the_api_key`
- `tests/test_api_client.py::test_redact_removes_the_api_key_from_urls`

## Pre-completion secret scan

Before reporting completion, scan the diff:

```bash
git diff | grep -inE "password\s*=|apikey=|api_key\s*=|secret\s*=|token\s*=|BEGIN (RSA|PRIVATE)|snowflakecomputing\.com"
```

Any hit must be explained or removed. Report the scan result in `validation.md`.

## If a secret is exposed

Stop, tell the developer which file/line, do not repeat the value, and
recommend rotating it. Do not attempt history rewriting without explicit instruction.
