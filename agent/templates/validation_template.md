# <JIRA-ID> — Validation

> Append-only. One section per change set. Status words: PASSED / FAILED /
> NOT RUN / NOT AVAILABLE. Never report PASSED for something not executed.

---

## Validation for change <n> — YYYY-MM-DD

Environment: <local Python x.y / Docker container / other>

| Check | Command / method | Status | Notes |
| --- | --- | --- | --- |
| Python syntax | `python -m py_compile ...` | | |
| Unit + static tests | `python -m pytest` | | <n passed, n skipped> |
| DAG static checks | `python -m pytest tests/test_dag_static.py` | | |
| DAG import (DagBag) | container pytest / `airflow dags list-import-errors` | | |
| SQL contract tests | `python -m pytest tests/test_transformations.py` | | |
| SQL manual review | object names, keys, binds, lineage columns | | |
| SQL execution in Snowflake | | NOT RUN | only on explicit request |
| Config validation | `python -m pytest tests/test_config.py` | | |
| Linting | — | NOT AVAILABLE | no linter configured |
| Secret scan of diff | `git diff \| grep -inE ...` (security.md) | | |
| Diff review | `git status`, `git diff` | | only planned files changed? |
| Docs updated | | | |

### Failures / output

```
<relevant output for FAILED checks>
```

### Residual risk

- <risk>
