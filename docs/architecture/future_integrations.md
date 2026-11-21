# Future tool integrations

**None of these are implemented.** This page describes where they would fit so
they can be added without restructuring the framework. Each would be its own
Jira ticket with approval (`agent/rules/development.md` §6).

Design principle: integrations **feed and read the same files** under
`agent/` — they do not replace them. The repository stays the source of truth;
tools make it faster to fill and check.

| Integration | What it would do | Touches | Prerequisites / open questions |
| --- | --- | --- | --- |
| **Jira** (e.g. an MCP server or REST client) | Validate that a Jira ID exists; pull title, description and acceptance criteria into `requirements.md` / `context.md`; post the `changes.md` summary and validation status back as a comment; optionally transition status | `agent/jira/<ID>/` | Jira site URL, project key, auth method (API token in env / secrets manager — never in repo), which transitions are allowed |
| **Git / GitHub** | Create a branch per ticket (`<JIRA-ID>-short-name`), commit with the Jira ID, open PRs with the changes summary | working tree, `changes.md` | Remote host, branch naming and PR template agreement; commits still only on explicit request |
| **Snowflake** (read-only first) | Verify objects referenced by new SQL exist, `DESCRIBE` tables, run `EXPLAIN` to syntax-check statements, run the README verification queries | `validation.md` | A dedicated **read-only role** (no such role exists today), credentials via the existing env-var mechanism; any write access needs separate approval |
| **Airflow** (REST API / CLI) | `dags list-import-errors`, trigger a test run in the local stack, read task logs for validation | `validation.md` | Docker stack running; API auth via existing `.env` UI user; never against shared environments without approval |
| **CI/CD** | Run `pytest` (and DagBag tests in the Airflow image) on every push; secret scan; later lint | `.github/workflows/` or equivalent (does not exist yet) | Choose CI host; decide on a linter; container-based job for Airflow tests |
| **Notifications** | Route failures already written to `AUDIT.ETL_RUN_LOG` to a channel | `src/orchestration/common.py` failure callback | Channel and secret mechanism not decided (see `agent/memory/global.md` open items) |

## Integration contract (to keep it clean)

1. Configuration and credentials for any tool come from environment variables
   or a secrets manager — `agent/rules/security.md` applies unchanged.
2. Tools write only into the documented files and sections (e.g. append to
   `changes.md`), using the same templates.
3. Read-only capabilities first; write capabilities (Jira transitions, git
   push, Snowflake DML) behind explicit developer confirmation.
4. If a tool is unavailable, the manual workflow in `CLAUDE.md` still works;
   the corresponding validation is reported as NOT RUN / NOT AVAILABLE.
