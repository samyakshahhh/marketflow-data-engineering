-- Record counts for the window being processed, split by validation outcome.
-- Used for the audit log so "rejected" is a measured number, not a difference.
SELECT
    COUNT(*)                AS TOTAL_RECORDS,
    COUNT_IF(IS_VALID)      AS VALID_RECORDS,
    COUNT_IF(NOT IS_VALID)  AS INVALID_RECORDS
FROM SILVER.VW_STOCK_PRICES_STAGING
WHERE INGESTED_AT >  TO_TIMESTAMP_NTZ(%(from_ts)s)
  AND INGESTED_AT <= TO_TIMESTAMP_NTZ(%(to_ts)s);
