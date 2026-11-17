/* ===========================================================================
   MarketFlow - 05 - Gold tables
   ---------------------------------------------------------------------------
   Analytical datasets derived from Silver with window functions.

   Column naming note: Snowflake identifiers cannot start with a digit unless
   quoted, so the moving averages are MOVING_AVG_7D / MOVING_AVG_30D and the
   rolling standard deviation of daily returns is VOLATILITY_30D.
   =========================================================================== */

USE DATABASE FINANCIAL_MARKET_DB;
USE SCHEMA GOLD;

CREATE TABLE IF NOT EXISTS DAILY_STOCK_PERFORMANCE (
    SYMBOL               VARCHAR(20)   NOT NULL,
    TRADE_DATE           DATE          NOT NULL,
    CLOSE_PRICE          NUMBER(18,6)  NOT NULL,
    PREV_CLOSE_PRICE     NUMBER(18,6)                COMMENT 'Previous trading day close; NULL for the first row of a symbol.',
    DAILY_RETURN         NUMBER(18,6)                COMMENT 'CLOSE_PRICE - PREV_CLOSE_PRICE.',
    DAILY_RETURN_PERCENT NUMBER(18,6)                COMMENT 'Daily return in percent; NULL when no usable previous close.',
    MOVING_AVG_7D        NUMBER(18,6)                COMMENT 'Average close over the last 7 trading days; NULL until 7 observations exist.',
    MOVING_AVG_30D       NUMBER(18,6)                COMMENT 'Average close over the last 30 trading days; NULL until 30 observations exist.',
    VOLATILITY_30D       NUMBER(18,6)                COMMENT 'Sample stddev of DAILY_RETURN_PERCENT over the last 30 trading days.',
    VOLUME               NUMBER(20,0),
    VWAP                 NUMBER(18,6),
    BATCH_ID             VARCHAR(64),
    RUN_ID               VARCHAR(250),
    CALCULATED_AT        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_GOLD_DAILY_STOCK_PERFORMANCE PRIMARY KEY (SYMBOL, TRADE_DATE)
)
COMMENT = 'Daily performance metrics per symbol and trading day.';

CREATE TABLE IF NOT EXISTS STOCK_PERFORMANCE_SUMMARY (
    SYMBOL              VARCHAR(20)   NOT NULL,
    PERIOD              VARCHAR(10)   NOT NULL      COMMENT 'Trailing window: 7D, 30D or 90D.',
    START_DATE          DATE,
    END_DATE            DATE,
    START_PRICE         NUMBER(18,6),
    END_PRICE           NUMBER(18,6),
    TOTAL_RETURN        NUMBER(18,6)                COMMENT 'Percentage return between START_PRICE and END_PRICE.',
    AVG_DAILY_RETURN    NUMBER(18,6)                COMMENT 'Mean of DAILY_RETURN_PERCENT within the window.',
    AVG_VOLUME          NUMBER(20,0),
    MAX_PRICE           NUMBER(18,6),
    MIN_PRICE           NUMBER(18,6),
    VOLATILITY          NUMBER(18,6)                COMMENT 'Sample stddev of DAILY_RETURN_PERCENT within the window.',
    TRADING_DAYS        NUMBER(10,0),
    BATCH_ID            VARCHAR(64),
    RUN_ID              VARCHAR(250),
    CALCULATED_AT       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_GOLD_STOCK_PERFORMANCE_SUMMARY PRIMARY KEY (SYMBOL, PERIOD)
)
COMMENT = 'Trailing-window performance summary, one row per symbol and period.';
