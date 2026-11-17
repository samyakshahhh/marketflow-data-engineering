-- Unprocessed Bronze company profile records and the upper bound of the window.
SELECT
    COUNT(*)         AS PENDING_RECORDS,
    MAX(INGESTED_AT) AS MAX_INGESTED_AT
FROM BRONZE.COMPANY_PROFILE_RAW
WHERE INGESTED_AT > TO_TIMESTAMP_NTZ(%(from_ts)s);
