-- Persist the data quality summary for this window: one row per check.
-- The same staging view that drives validation drives the counts, so the
-- numbers always match what was quarantined and merged.
INSERT INTO AUDIT.DATA_QUALITY_RESULTS
    (RUN_ID, BATCH_ID, TABLE_NAME, CHECK_NAME, EXPECTED_RESULT, ACTUAL_RESULT,
     RECORDS_CHECKED, RECORDS_FAILED, STATUS, CHECKED_AT)
WITH scoped AS (
    SELECT *
    FROM SILVER.VW_STOCK_PRICES_STAGING
    WHERE INGESTED_AT >  TO_TIMESTAMP_NTZ(%(from_ts)s)
      AND INGESTED_AT <= TO_TIMESTAMP_NTZ(%(to_ts)s)
),
checks AS (
    SELECT 'NULL_REQUIRED_FIELD' AS CHECK_NAME,
           COUNT(*)                            AS RECORDS_CHECKED,
           COUNT_IF(HAS_NULL_REQUIRED_FIELD)   AS RECORDS_FAILED
    FROM scoped
    UNION ALL
    SELECT 'DATE_VALIDATION', COUNT(*), COUNT_IF(HAS_INVALID_DATE)   FROM scoped
    UNION ALL
    SELECT 'PRICE_RANGE',     COUNT(*), COUNT_IF(HAS_INVALID_PRICE)  FROM scoped
    UNION ALL
    SELECT 'VOLUME_RANGE',    COUNT(*), COUNT_IF(HAS_INVALID_VOLUME) FROM scoped
    UNION ALL
    SELECT 'DUPLICATES',
           COUNT(*),
           COUNT(*) - COUNT(DISTINCT SYMBOL || '#' || TO_VARCHAR(TRADE_DATE))
    FROM scoped
    WHERE IS_VALID
)
SELECT
    %(run_id)s,
    %(batch_id)s,
    'SILVER.STOCK_PRICES',
    CHECK_NAME,
    'failing records = 0',
    TO_VARCHAR(RECORDS_FAILED) || ' of ' || TO_VARCHAR(RECORDS_CHECKED) || ' records failed',
    RECORDS_CHECKED,
    RECORDS_FAILED,
    IFF(RECORDS_FAILED = 0, 'PASS', 'FAIL'),
    CURRENT_TIMESTAMP()
FROM checks;
