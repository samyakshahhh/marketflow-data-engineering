/* ===========================================================================
   MarketFlow - 04 - Silver tables
   ---------------------------------------------------------------------------
   Typed and validated records. Business key: SYMBOL + TRADE_DATE.
   Snowflake does not enforce PRIMARY KEY constraints; they are declared for
   documentation and for BI tools that read the metadata.
   =========================================================================== */

USE DATABASE FINANCIAL_MARKET_DB;
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS STOCK_PRICES (
    SYMBOL          VARCHAR(20)     NOT NULL,
    TRADE_DATE      DATE            NOT NULL,
    OPEN_PRICE      NUMBER(18,6),
    HIGH_PRICE      NUMBER(18,6),
    LOW_PRICE       NUMBER(18,6),
    CLOSE_PRICE     NUMBER(18,6)    NOT NULL,
    VOLUME          NUMBER(20,0),
    CHANGE_AMOUNT   NUMBER(18,6)                  COMMENT 'Absolute change reported by the provider.',
    CHANGE_PERCENT  NUMBER(18,6)                  COMMENT 'Percentage change reported by the provider.',
    VWAP            NUMBER(18,6),
    SOURCE_SYSTEM   VARCHAR(50),
    BATCH_ID        VARCHAR(64)                   COMMENT 'Batch that last wrote this row.',
    RUN_ID          VARCHAR(250),
    INGESTED_AT     TIMESTAMP_NTZ                 COMMENT 'Bronze ingestion timestamp of the winning record.',
    LOADED_AT       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'Silver write timestamp; drives the Gold watermark.',
    CONSTRAINT PK_SILVER_STOCK_PRICES PRIMARY KEY (SYMBOL, TRADE_DATE)
)
COMMENT = 'Clean daily price history, one row per symbol and trading day.';

CREATE TABLE IF NOT EXISTS COMPANY_PROFILE (
    SYMBOL          VARCHAR(20)     NOT NULL,
    COMPANY_NAME    VARCHAR(250),
    INDUSTRY        VARCHAR(250),
    SECTOR          VARCHAR(250),
    EXCHANGE        VARCHAR(250),
    CURRENCY        VARCHAR(10),
    COUNTRY         VARCHAR(100),
    MARKET_CAP      NUMBER(38,0),
    SOURCE_SYSTEM   VARCHAR(50),
    BATCH_ID        VARCHAR(64),
    RUN_ID          VARCHAR(250),
    INGESTED_AT     TIMESTAMP_NTZ,
    LOADED_AT       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SILVER_COMPANY_PROFILE PRIMARY KEY (SYMBOL)
)
COMMENT = 'Current company metadata, one row per symbol (secondary pipeline).';
