/* ===========================================================================
   MarketFlow - 01 - Database
   ---------------------------------------------------------------------------
   Required privileges: a role that may create databases (SYSADMIN works on a
   standard Snowflake trial account).

   If your role cannot create databases, ask an administrator to create
   FINANCIAL_MARKET_DB and grant you OWNERSHIP/USAGE on it, then start from
   script 02.

   All scripts are re-runnable (CREATE ... IF NOT EXISTS / OR REPLACE).
   =========================================================================== */

CREATE DATABASE IF NOT EXISTS FINANCIAL_MARKET_DB
    COMMENT = 'MarketFlow: end-to-end financial market data platform (Bronze/Silver/Gold/Audit).';

/* A warehouse is required to run the pipeline. Most accounts already have
   COMPUTE_WH; uncomment if you need a dedicated one and your role may create it.

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse used by the MarketFlow pipeline.';
*/
