-- Rows written by the current run, used for the audit log.
SELECT
    (SELECT COUNT(*) FROM GOLD.DAILY_STOCK_PERFORMANCE  WHERE RUN_ID = %(run_id)s) AS DAILY_ROWS,
    (SELECT COUNT(*) FROM GOLD.STOCK_PERFORMANCE_SUMMARY WHERE RUN_ID = %(run_id)s) AS SUMMARY_ROWS;
