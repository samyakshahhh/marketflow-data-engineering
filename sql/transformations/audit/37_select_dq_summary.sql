-- Data quality summary for one run and target, used for task logging.
SELECT
    CHECK_NAME,
    STATUS,
    RECORDS_CHECKED,
    RECORDS_FAILED
FROM AUDIT.DATA_QUALITY_RESULTS
WHERE RUN_ID = %(run_id)s
  AND TABLE_NAME = %(table_name)s
ORDER BY CHECK_NAME;
