-- Quarantine the losing side of a duplicate: valid records that share
-- SYMBOL + TRADE_DATE with a more recently ingested record in the same window.
-- The winner is merged into Silver by 13_merge_stock_prices.sql.
INSERT INTO AUDIT.DATA_QUALITY_ERRORS
    (RUN_ID, BATCH_ID, TABLE_NAME, SYMBOL, TRADE_DATE, ERROR_TYPE, ERROR_MESSAGE, RAW_RECORD, FAILED_AT)
WITH scoped AS (
    SELECT *
    FROM SILVER.VW_STOCK_PRICES_STAGING
    WHERE INGESTED_AT >  TO_TIMESTAMP_NTZ(%(from_ts)s)
      AND INGESTED_AT <= TO_TIMESTAMP_NTZ(%(to_ts)s)
      AND IS_VALID
),
ranked AS (
    SELECT
        scoped.*,
        ROW_NUMBER() OVER (
            PARTITION BY SYMBOL, TRADE_DATE
            ORDER BY INGESTED_AT DESC, RAW_ID DESC
        ) AS RECORD_RANK
    FROM scoped
)
SELECT
    %(run_id)s,
    BATCH_ID,
    'SILVER.STOCK_PRICES',
    SYMBOL,
    TRADE_DATE,
    'DUPLICATE_RECORD',
    'superseded by a more recently ingested record for the same symbol and trade date',
    RAW_DATA,
    CURRENT_TIMESTAMP()
FROM ranked
WHERE RECORD_RANK > 1;
