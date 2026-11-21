# Airflow documentation

| Topic | Where |
| --- | --- |
| DAG overview and dependency graph | [`../../README.md`](../../README.md) *The DAGs* |
| Task-by-task walkthrough of each DAG | [`../DATA_FLOW.md`](../DATA_FLOW.md) §5 *Airflow Architecture* |
| Retries, failures, one symbol failing | [`../DATA_FLOW.md`](../DATA_FLOW.md) §13 *Error Handling* |
| Docker / services | [`../DATA_FLOW.md`](../DATA_FLOW.md) §15, `docker-compose.yml` |
| Running and triggering DAGs | README *Local setup* §3–4 |
| **Conventions the agent must follow** | [`../../agent/rules/airflow.md`](../../agent/rules/airflow.md) |
| In-code documentation | Each DAG's module docstring (rendered in the Airflow UI via `doc_md`) |

Current DAGs: `market_api_to_bronze` (cron, weekdays 06:00), `bronze_to_silver`
(Bronze asset), `silver_to_gold` (Silver asset), `company_profile_ingestion`
(cron, Mondays 07:00).
