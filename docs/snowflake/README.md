# Snowflake documentation

| Topic | Where |
| --- | --- |
| Object inventory (database, schemas, tables, views) | [`../../README.md`](../../README.md) *Snowflake architecture* |
| Data model and columns | [`../DATA_FLOW.md`](../DATA_FLOW.md) §14 *Snowflake Data Model*; DDL in `sql/01..07_*.sql` |
| Bronze / Silver / Gold logic | [`../DATA_FLOW.md`](../DATA_FLOW.md) §6, §7, §9 |
| Incremental processing and watermarks | README *Incremental loading*; DATA_FLOW §10 |
| Audit tables | DATA_FLOW §11 |
| Required privileges | README *Required privileges* |
| Verification queries | README *Verifying a run in Snowflake*; DATA_FLOW §18 |
| Business meaning of columns and calculations | [`../business/data_definitions.md`](../business/data_definitions.md) |
| **Conventions the agent must follow** | [`../../agent/rules/snowflake.md`](../../agent/rules/snowflake.md) |

Setup: run `sql/01..07` in order in Snowsight, or
`docker compose run --rm airflow-cli python /opt/airflow/project/scripts/setup_snowflake.py`.
