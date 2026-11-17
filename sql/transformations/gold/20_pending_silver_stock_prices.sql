-- Silver rows written since the Gold watermark, and the upper bound of the
-- window the current Gold run should process.
SELECT
    COUNT(*)        AS PENDING_RECORDS,
    MAX(LOADED_AT)  AS MAX_LOADED_AT
FROM SILVER.STOCK_PRICES
WHERE LOADED_AT > TO_TIMESTAMP_NTZ(%(from_ts)s);
