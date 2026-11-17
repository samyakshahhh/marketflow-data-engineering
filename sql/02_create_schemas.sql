/* ===========================================================================
   MarketFlow - 02 - Schemas
   ---------------------------------------------------------------------------
   One database, four schemas (medallion architecture + audit).
   =========================================================================== */

USE DATABASE FINANCIAL_MARKET_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw, append-only API payloads. Nothing is modified or deleted here.';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Typed, validated and de-duplicated business records.';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Analytical datasets ready for BI consumption.';

CREATE SCHEMA IF NOT EXISTS AUDIT
    COMMENT = 'ETL run log, data quality results, quarantined records and load watermarks.';
