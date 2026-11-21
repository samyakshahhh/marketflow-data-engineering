# Snowflake rules

Derived from `sql/01..07_*.sql`, `sql/transformations/**`, `src/warehouse/`,
`config/pipeline.yaml` and `tests/test_transformations.py`. Do not invent
Snowflake objects: the full current inventory is in README "Snowflake
architecture" and `docs/DATA_FLOW.md` §14.

---

## Database and schemas

| Object | Name | Purpose |
| --- | --- | --- |
| Database | `FINANCIAL_MARKET_DB` | everything |
| Schema | `BRONZE` | raw API payloads, **append-only** — never update or delete |
| Schema | `SILVER` | typed, validated, de-duplicated business records + staging views |
| Schema | `GOLD` | analytical datasets + BI view |
| Schema | `AUDIT` | run log, DQ results, quarantine, watermarks |

Names are configured in `config/pipeline.yaml` (`snowflake:`); `SNOWFLAKE_DATABASE`
env var overrides the database. A new schema or layer needs approval.

## Existing objects

- BRONZE: `STOCK_PRICES_RAW`, `COMPANY_PROFILE_RAW`
- SILVER: `STOCK_PRICES`, `COMPANY_PROFILE`, `VW_STOCK_PRICES_STAGING`, `VW_COMPANY_PROFILE_STAGING`
- GOLD: `DAILY_STOCK_PERFORMANCE`, `STOCK_PERFORMANCE_SUMMARY`, `VW_STOCK_PERFORMANCE`
- AUDIT: `ETL_RUN_LOG`, `DATA_QUALITY_RESULTS`, `DATA_QUALITY_ERRORS`, `LOAD_WATERMARK`
- **None exist** of: stored procedures, UDFs, tasks, streams, stages, pipes,
  file formats, roles/grants scripts. Introducing any requires approval.

## Naming

- All identifiers `UPPER_SNAKE_CASE`, unquoted.
- Bronze tables: `<ENTITY>_RAW`. Silver: plain entity name (`STOCK_PRICES`).
  Gold: descriptive analytical name (`DAILY_STOCK_PERFORMANCE`).
- Views: `VW_` prefix. Bronze→Silver parsing/validation views: `SILVER.VW_<ENTITY>_STAGING`.
- Primary key constraints: `PK_<SCHEMA>_<TABLE>` (declared for documentation/BI;
  Snowflake does not enforce them — uniqueness comes from the MERGE).
- Identifiers cannot start with a digit: use suffixes (`MOVING_AVG_7D`,
  `VOLATILITY_30D`), never `7D_...`.
- Price columns end in `_PRICE`; percentages end in `_PERCENT`.

## Data types (established)

| Kind | Type |
| --- | --- |
| Symbol | `VARCHAR(20)` |
| Prices, returns, ratios | `NUMBER(18,6)` |
| Volume | `NUMBER(20,0)` |
| Market cap | `NUMBER(38,0)` |
| Descriptive text | `VARCHAR(250)` (country `VARCHAR(100)`, currency `VARCHAR(10)`) |
| `BATCH_ID` | `VARCHAR(64)` (uuid4 hex) |
| `RUN_ID`, `DAG_ID`, `TASK_ID`, `TABLE_NAME` | `VARCHAR(250)` |
| Raw payload | `VARIANT` |
| Timestamps | `TIMESTAMP_NTZ`, UTC, default `CURRENT_TIMESTAMP()` |
| Business dates | `DATE` |

## Lineage / audit columns

| Layer | Required columns |
| --- | --- |
| Bronze | `RAW_ID NUMBER(38,0) AUTOINCREMENT`, `RAW_DATA VARIANT NOT NULL`, `SYMBOL`, `SOURCE_SYSTEM`, `API_ENDPOINT` (no credentials), `BATCH_ID`, `RUN_ID`, `INGESTED_AT` |
| Silver | business columns + `SOURCE_SYSTEM`, `BATCH_ID`, `RUN_ID`, `INGESTED_AT` (from Bronze winner), `LOADED_AT` (Silver write time, drives Gold watermark) |
| Gold | business columns + `BATCH_ID`, `RUN_ID` (of the Gold run), `CALCULATED_AT` |

Every table has a `COMMENT`; non-obvious columns have column comments.

## Business keys

- `SILVER.STOCK_PRICES`, `GOLD.DAILY_STOCK_PERFORMANCE`: `(SYMBOL, TRADE_DATE)`
- `SILVER.COMPANY_PROFILE`: `(SYMBOL)`
- `GOLD.STOCK_PERFORMANCE_SUMMARY`: `(SYMBOL, PERIOD)` with `PERIOD` in `7D`/`30D`/`90D`
- `AUDIT.ETL_RUN_LOG`: `(RUN_ID, DAG_ID, TASK_ID)`; `AUDIT.LOAD_WATERMARK`: `(LAYER, TABLE_NAME, SYMBOL)`

Changing a business key requires approval.

## DDL files (`sql/01..07_*.sql`)

- Numbered, run in order, by hand in Snowsight or by `scripts/setup_snowflake.py`
  (which runs every `sql/0*.sql` in sorted order).
- Re-runnable: `CREATE ... IF NOT EXISTS` for tables/schemas, `CREATE OR REPLACE VIEW` for views.
- Header block comment (`/* ===...=== MarketFlow - NN - Title ... */`) explaining purpose.
- New table in an existing layer → add to that layer's file. Genuinely new
  object kind → new next-numbered file (`08_...`).
- **No migration tool exists.** `IF NOT EXISTS` will not alter an existing
  table; column changes need an explicit, approved `ALTER` script and must be
  recorded in the Jira `decisions.md`.

## DML files (`sql/transformations/<layer>/`)

- **One statement per file**; Python only binds parameters (`src/warehouse/loader.py`).
- Numbering by layer: `silver/` 10–19, `gold/` 20–29, `audit/` 30–39. Next free
  numbers today: silver `18`, gold `24`, audit `39`. Bronze insert templates
  are un-numbered (`insert_<entity>_raw.sql`) and use the `{value_rows}` placeholder.
- Start each file with `--` comments explaining what and why.
- Parameters: named binds `%(name)s` only (positional `%s` only in the generated Bronze `VALUES`).
- **No literal `%` anywhere** — including comments (the connector binds across
  the whole text; `tests/test_transformations.py::test_sql_uses_bind_parameters_only`).
- No credentials or the words `password`, `apikey`, `api_key`, `secret` (test-enforced).
- Every `.sql` path referenced from `src/` must exist (test-enforced).

## Incremental processing

- Control table `AUDIT.LOAD_WATERMARK`, accessed only via
  `audit.get_watermark()` / `audit.set_watermark()`. Never use `MAX(date)` of a target table.
- Bronze watermark: `LAST_LOADED_DATE` per symbol. Silver/Gold: `LAST_PROCESSED_TS`
  with `SYMBOL = '_ALL_'`.
- Window: `(from_ts, to_ts]` on `INGESTED_AT` (Bronze→Silver) or `LOADED_AT`
  (Silver→Gold); `to_ts` pinned once at planning time by a `pending` query
  returning `PENDING_RECORDS` and `MAX_<TS>`; first run uses `EPOCH_TS = '1900-01-01 00:00:00'`.
- Watermark advanced only after the step succeeded.

## MERGE pattern

- `MERGE INTO <target> t USING (<source> ) s ON <business key>`.
- De-duplicate the source with
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY INGESTED_AT DESC, RAW_ID DESC) = 1`.
- Silver guards against older data overwriting newer:
  `WHEN MATCHED AND s.INGESTED_AT >= COALESCE(t.INGESTED_AT, TO_TIMESTAMP_NTZ('1900-01-01'))`.
- Set `LOADED_AT` / `CALCULATED_AT = CURRENT_TIMESTAMP()` on both branches.
- Gold casts calculated values to the target type (`CAST(... AS NUMBER(18,6))`)
  and guards division with `NULLIF(x, 0)`.

## Validation and quarantine

- Typing and business validation live in the staging view
  (`SILVER.VW_STOCK_PRICES_STAGING`): `TRY_TO_*` parsing, `HAS_*` boolean flags,
  an `ERRORS` array of `{type, message}`, and `IS_VALID`. Quarantine, DQ summary
  and MERGE all read that one view — add a new rule there, not in three places.
- Invalid and duplicate records go to `AUDIT.DATA_QUALITY_ERRORS` with the raw
  payload; they are never dropped.
- Error types: `NULL_REQUIRED_FIELD`, `INVALID_DATE`, `INVALID_PRICE`,
  `INVALID_VOLUME`, `DUPLICATE_RECORD`, `INVALID_API_RECORD`.
- Check names in `DATA_QUALITY_RESULTS`: `NULL_REQUIRED_FIELD`, `DATE_VALIDATION`,
  `PRICE_RANGE`, `VOLUME_RANGE`, `DUPLICATES`; status `PASS`/`FAIL`.
- Retries are idempotent: DQ rows for a `RUN_ID` + table are deleted before being rewritten.

## Transactions

Statements that must land together run inside
`with transaction(connection):` (`BEGIN` / `COMMIT` / `ROLLBACK`), as in
`bronze_to_silver.transform_to_silver`.

## Roles, warehouses, security

- Role from `SNOWFLAKE_ROLE` (template default `SYSADMIN`); warehouse from
  `SNOWFLAKE_WAREHOUSE` (default `COMPUTE_WH`). Never hardcode either in SQL or Python.
- Pipeline privileges required are listed in README "Required privileges".
- No grants script / least-privilege role exists yet (an open decision noted
  by the project owner). Do not create roles or grants without approval.
- Never put account identifiers, users or passwords in SQL files.
