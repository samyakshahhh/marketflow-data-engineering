/* ===========================================================================
   MarketFlow - 07 - Views
   ---------------------------------------------------------------------------
   1. SILVER.VW_STOCK_PRICES_STAGING   - types and validates Bronze records.
   2. SILVER.VW_COMPANY_PROFILE_STAGING- types Bronze company profiles.
   3. GOLD.VW_STOCK_PERFORMANCE        - BI-facing consumption view.

   The staging views hold the validation rules in ONE place: the quarantine
   statement, the data quality summary and the Silver MERGE all read them, so
   the three can never disagree about what "valid" means.

   Row level flags only - de-duplication is done by the MERGE statement,
   because the de-duplication window depends on the batch being processed.
   =========================================================================== */

USE DATABASE FINANCIAL_MARKET_DB;

CREATE OR REPLACE VIEW SILVER.VW_STOCK_PRICES_STAGING
COMMENT = 'Bronze stock price payloads parsed into typed columns with validation flags.'
AS
WITH parsed AS (
    SELECT
        r.RAW_ID,
        r.RAW_DATA,
        r.BATCH_ID,
        r.RUN_ID,
        r.SOURCE_SYSTEM,
        r.API_ENDPOINT,
        r.INGESTED_AT,
        UPPER(COALESCE(r.RAW_DATA:symbol::VARCHAR, r.SYMBOL))      AS SYMBOL,
        TRY_TO_DATE(r.RAW_DATA:date::VARCHAR)                      AS TRADE_DATE,
        TRY_TO_DECIMAL(r.RAW_DATA:open::VARCHAR, 18, 6)            AS OPEN_PRICE,
        TRY_TO_DECIMAL(r.RAW_DATA:high::VARCHAR, 18, 6)            AS HIGH_PRICE,
        TRY_TO_DECIMAL(r.RAW_DATA:low::VARCHAR, 18, 6)             AS LOW_PRICE,
        TRY_TO_DECIMAL(r.RAW_DATA:close::VARCHAR, 18, 6)           AS CLOSE_PRICE,
        TRY_TO_NUMBER(r.RAW_DATA:volume::VARCHAR, 20, 0)           AS VOLUME,
        TRY_TO_DECIMAL(r.RAW_DATA:change::VARCHAR, 18, 6)          AS CHANGE_AMOUNT,
        TRY_TO_DECIMAL(r.RAW_DATA:changePercent::VARCHAR, 18, 6)   AS CHANGE_PERCENT,
        TRY_TO_DECIMAL(r.RAW_DATA:vwap::VARCHAR, 18, 6)            AS VWAP
    FROM BRONZE.STOCK_PRICES_RAW r
),
flagged AS (
    SELECT
        p.*,
        /* Required business fields. */
        (p.SYMBOL IS NULL OR p.TRADE_DATE IS NULL OR p.CLOSE_PRICE IS NULL)           AS HAS_NULL_REQUIRED_FIELD,
        /* Unparseable, future or implausibly old trade dates. */
        (p.TRADE_DATE IS NULL
            OR p.TRADE_DATE > CURRENT_DATE()
            OR p.TRADE_DATE < DATE '1990-01-01')                                      AS HAS_INVALID_DATE,
        /* OHLC relationships. A missing optional price leaves the comparison
           unknown, which is not treated as a failure; a missing or non positive
           close always is. */
        NOT (COALESCE(p.HIGH_PRICE >= p.LOW_PRICE
                      AND p.HIGH_PRICE >= p.OPEN_PRICE
                      AND p.HIGH_PRICE >= p.CLOSE_PRICE
                      AND p.LOW_PRICE  <= p.OPEN_PRICE
                      AND p.LOW_PRICE  <= p.CLOSE_PRICE, TRUE)
             AND COALESCE(p.CLOSE_PRICE > 0, FALSE))                                  AS HAS_INVALID_PRICE,
        NOT COALESCE(p.VOLUME >= 0, FALSE)                                            AS HAS_INVALID_VOLUME
    FROM parsed p
)
SELECT
    f.*,
    ARRAY_CONSTRUCT_COMPACT(
        IFF(f.HAS_NULL_REQUIRED_FIELD,
            OBJECT_CONSTRUCT('type', 'NULL_REQUIRED_FIELD',
                             'message', 'SYMBOL, TRADE_DATE or CLOSE_PRICE is null'), NULL),
        IFF(f.HAS_INVALID_DATE,
            OBJECT_CONSTRUCT('type', 'INVALID_DATE',
                             'message', 'trade date is missing, unparseable, in the future or before 1990-01-01'), NULL),
        IFF(f.HAS_INVALID_PRICE,
            OBJECT_CONSTRUCT('type', 'INVALID_PRICE',
                             'message', 'close price is not positive or OHLC relationships are violated'), NULL),
        IFF(f.HAS_INVALID_VOLUME,
            OBJECT_CONSTRUCT('type', 'INVALID_VOLUME',
                             'message', 'volume is missing or negative'), NULL)
    )                                                                                 AS ERRORS,
    NOT (f.HAS_NULL_REQUIRED_FIELD
         OR f.HAS_INVALID_DATE
         OR f.HAS_INVALID_PRICE
         OR f.HAS_INVALID_VOLUME)                                                     AS IS_VALID
FROM flagged f;

CREATE OR REPLACE VIEW SILVER.VW_COMPANY_PROFILE_STAGING
COMMENT = 'Bronze company profile payloads parsed into typed columns.'
AS
SELECT
    r.RAW_ID,
    r.RAW_DATA,
    r.BATCH_ID,
    r.RUN_ID,
    r.SOURCE_SYSTEM,
    r.INGESTED_AT,
    UPPER(COALESCE(r.RAW_DATA:symbol::VARCHAR, r.SYMBOL))                   AS SYMBOL,
    r.RAW_DATA:companyName::VARCHAR                                         AS COMPANY_NAME,
    r.RAW_DATA:industry::VARCHAR                                            AS INDUSTRY,
    r.RAW_DATA:sector::VARCHAR                                              AS SECTOR,
    COALESCE(r.RAW_DATA:exchangeFullName::VARCHAR,
             r.RAW_DATA:exchange::VARCHAR)                                  AS EXCHANGE,
    r.RAW_DATA:currency::VARCHAR                                            AS CURRENCY,
    r.RAW_DATA:country::VARCHAR                                             AS COUNTRY,
    TRY_TO_NUMBER(r.RAW_DATA:marketCap::VARCHAR, 38, 0)                     AS MARKET_CAP
FROM BRONZE.COMPANY_PROFILE_RAW r;

CREATE OR REPLACE VIEW GOLD.VW_STOCK_PERFORMANCE
COMMENT = 'Business facing view: daily performance enriched with company metadata and trailing 30 day summary. Point a BI tool here.'
AS
SELECT
    d.SYMBOL,
    c.COMPANY_NAME,
    c.SECTOR,
    c.INDUSTRY,
    c.EXCHANGE,
    c.CURRENCY,
    d.TRADE_DATE,
    d.CLOSE_PRICE,
    d.PREV_CLOSE_PRICE,
    d.DAILY_RETURN,
    d.DAILY_RETURN_PERCENT,
    d.MOVING_AVG_7D,
    d.MOVING_AVG_30D,
    d.VOLATILITY_30D,
    d.VOLUME,
    d.VWAP,
    s.TOTAL_RETURN      AS TOTAL_RETURN_30D,
    s.AVG_DAILY_RETURN  AS AVG_DAILY_RETURN_30D,
    s.AVG_VOLUME        AS AVG_VOLUME_30D,
    s.VOLATILITY        AS VOLATILITY_SUMMARY_30D,
    d.BATCH_ID,
    d.RUN_ID,
    d.CALCULATED_AT
FROM GOLD.DAILY_STOCK_PERFORMANCE d
LEFT JOIN SILVER.COMPANY_PROFILE c
       ON c.SYMBOL = d.SYMBOL
LEFT JOIN GOLD.STOCK_PERFORMANCE_SUMMARY s
       ON s.SYMBOL = d.SYMBOL
      AND s.PERIOD = '30D';
