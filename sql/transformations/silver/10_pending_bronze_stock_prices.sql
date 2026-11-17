-- How much unprocessed Bronze data is waiting, and up to which ingestion
-- timestamp the current run should process. Taking MAX(INGESTED_AT) once gives
-- every statement in the run the same, stable window.
SELECT
    COUNT(*)            AS PENDING_RECORDS,
    MAX(INGESTED_AT)    AS MAX_INGESTED_AT
FROM BRONZE.STOCK_PRICES_RAW
WHERE INGESTED_AT > TO_TIMESTAMP_NTZ(%(from_ts)s);
