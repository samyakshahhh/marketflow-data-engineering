-- Quarantine records that fail validation: one row per record and error type,
-- with the original payload attached so it can be inspected or replayed.
INSERT INTO AUDIT.DATA_QUALITY_ERRORS
    (RUN_ID, BATCH_ID, TABLE_NAME, SYMBOL, TRADE_DATE, ERROR_TYPE, ERROR_MESSAGE, RAW_RECORD, FAILED_AT)
SELECT
    %(run_id)s,
    s.BATCH_ID,
    'SILVER.STOCK_PRICES',
    s.SYMBOL,
    s.TRADE_DATE,
    e.value:type::VARCHAR,
    e.value:message::VARCHAR,
    s.RAW_DATA,
    CURRENT_TIMESTAMP()
FROM SILVER.VW_STOCK_PRICES_STAGING s,
     LATERAL FLATTEN(input => s.ERRORS) e
WHERE s.INGESTED_AT >  TO_TIMESTAMP_NTZ(%(from_ts)s)
  AND s.INGESTED_AT <= TO_TIMESTAMP_NTZ(%(to_ts)s);
