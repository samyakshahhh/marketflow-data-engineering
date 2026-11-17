/* ===========================================================================
   MarketFlow - 03 - Bronze tables
   ---------------------------------------------------------------------------
   Bronze keeps the API response as it was received (VARIANT) plus the
   metadata needed to trace a record back to the run that produced it.
   Append-only: Silver and Gold can always be rebuilt from here.
   =========================================================================== */

USE DATABASE FINANCIAL_MARKET_DB;
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS STOCK_PRICES_RAW (
    RAW_ID          NUMBER(38,0)    AUTOINCREMENT START 1 INCREMENT 1 COMMENT 'Surrogate key, also breaks ties when de-duplicating.',
    RAW_DATA        VARIANT         NOT NULL      COMMENT 'One end-of-day price record exactly as returned by the API.',
    SYMBOL          VARCHAR(20)     NOT NULL      COMMENT 'Symbol the record was requested for.',
    SOURCE_SYSTEM   VARCHAR(50)     NOT NULL      COMMENT 'Data provider, e.g. FMP.',
    API_ENDPOINT    VARCHAR(500)                  COMMENT 'Endpoint path the payload came from (no credentials).',
    BATCH_ID        VARCHAR(64)     NOT NULL      COMMENT 'Unique id of the ingestion batch.',
    RUN_ID          VARCHAR(250)    NOT NULL      COMMENT 'Airflow dag_run id that produced the batch.',
    INGESTED_AT     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'Load timestamp; drives the Silver watermark.'
)
COMMENT = 'Raw end-of-day stock prices (append-only).';

CREATE TABLE IF NOT EXISTS COMPANY_PROFILE_RAW (
    RAW_ID          NUMBER(38,0)    AUTOINCREMENT START 1 INCREMENT 1,
    RAW_DATA        VARIANT         NOT NULL      COMMENT 'Company profile payload as returned by the API.',
    SYMBOL          VARCHAR(20)     NOT NULL,
    SOURCE_SYSTEM   VARCHAR(50)     NOT NULL,
    API_ENDPOINT    VARCHAR(500),
    BATCH_ID        VARCHAR(64)     NOT NULL,
    RUN_ID          VARCHAR(250)    NOT NULL,
    INGESTED_AT     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw company profile metadata (append-only, secondary pipeline).';
