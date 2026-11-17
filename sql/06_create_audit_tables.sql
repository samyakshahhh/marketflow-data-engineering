/* ===========================================================================
   MarketFlow - 06 - Audit tables
   ---------------------------------------------------------------------------
   Every task writes here: run log, data quality results, quarantined records
   and the watermarks that drive incremental processing.
   =========================================================================== */

USE DATABASE FINANCIAL_MARKET_DB;
USE SCHEMA AUDIT;

CREATE TABLE IF NOT EXISTS ETL_RUN_LOG (
    RUN_ID                  VARCHAR(250)  NOT NULL  COMMENT 'Airflow dag_run id.',
    DAG_ID                  VARCHAR(250)  NOT NULL,
    TASK_ID                 VARCHAR(250)  NOT NULL  COMMENT 'Task, or a logical step name for mapped tasks (task_id[symbol]).',
    BATCH_ID                VARCHAR(64),
    LAYER                   VARCHAR(20)             COMMENT 'BRONZE, SILVER, GOLD or AUDIT.',
    START_TIME              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    END_TIME                TIMESTAMP_NTZ,
    DURATION_SECONDS        NUMBER(18,3),
    STATUS                  VARCHAR(20)   NOT NULL  COMMENT 'RUNNING, SUCCESS, FAILED or SKIPPED.',
    SOURCE_RECORD_COUNT     NUMBER(18,0),
    TARGET_RECORD_COUNT     NUMBER(18,0),
    REJECTED_RECORD_COUNT   NUMBER(18,0),
    ERROR_MESSAGE           VARCHAR(5000),
    UPDATED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_AUDIT_ETL_RUN_LOG PRIMARY KEY (RUN_ID, DAG_ID, TASK_ID)
)
COMMENT = 'One row per executed pipeline step.';

CREATE TABLE IF NOT EXISTS DATA_QUALITY_RESULTS (
    RESULT_ID        NUMBER(38,0)  AUTOINCREMENT START 1 INCREMENT 1,
    RUN_ID           VARCHAR(250)  NOT NULL,
    BATCH_ID         VARCHAR(64),
    TABLE_NAME       VARCHAR(250)  NOT NULL  COMMENT 'Target the check was executed for, e.g. SILVER.STOCK_PRICES.',
    CHECK_NAME       VARCHAR(100)  NOT NULL  COMMENT 'NULL_REQUIRED_FIELD, DUPLICATES, PRICE_RANGE, VOLUME_RANGE, DATE_VALIDATION.',
    EXPECTED_RESULT  VARCHAR(500),
    ACTUAL_RESULT    VARCHAR(500),
    RECORDS_CHECKED  NUMBER(18,0),
    RECORDS_FAILED   NUMBER(18,0),
    STATUS           VARCHAR(10)   NOT NULL  COMMENT 'PASS or FAIL.',
    CHECKED_AT       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Outcome of every data quality check, one row per check and run.';

CREATE TABLE IF NOT EXISTS DATA_QUALITY_ERRORS (
    ERROR_ID      NUMBER(38,0)  AUTOINCREMENT START 1 INCREMENT 1,
    RUN_ID        VARCHAR(250)  NOT NULL,
    BATCH_ID      VARCHAR(64),
    TABLE_NAME    VARCHAR(250)  NOT NULL  COMMENT 'Target the record was rejected for.',
    SYMBOL        VARCHAR(20),
    TRADE_DATE    DATE,
    ERROR_TYPE    VARCHAR(50)   NOT NULL  COMMENT 'NULL_REQUIRED_FIELD, DUPLICATE_RECORD, INVALID_PRICE, INVALID_VOLUME, INVALID_DATE, INVALID_API_RECORD.',
    ERROR_MESSAGE VARCHAR(1000),
    RAW_RECORD    VARIANT                 COMMENT 'The rejected record, kept so it can be inspected or replayed.',
    FAILED_AT     TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Quarantine: rejected records are stored here instead of being dropped.';

CREATE TABLE IF NOT EXISTS LOAD_WATERMARK (
    LAYER             VARCHAR(20)   NOT NULL  COMMENT 'BRONZE, SILVER or GOLD.',
    TABLE_NAME        VARCHAR(250)  NOT NULL  COMMENT 'Target table the watermark belongs to.',
    SYMBOL            VARCHAR(20)   NOT NULL  COMMENT 'Symbol, or _ALL_ for table level watermarks.',
    LAST_LOADED_DATE  DATE                    COMMENT 'Latest trade date successfully ingested (Bronze).',
    LAST_PROCESSED_TS TIMESTAMP_NTZ           COMMENT 'Latest source timestamp successfully processed (Silver/Gold).',
    LAST_RUN_ID       VARCHAR(250),
    LAST_BATCH_ID     VARCHAR(64),
    UPDATED_AT        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_AUDIT_LOAD_WATERMARK PRIMARY KEY (LAYER, TABLE_NAME, SYMBOL)
)
COMMENT = 'Control table: only updated after a step succeeds, so a failed run never advances the pipeline.';
