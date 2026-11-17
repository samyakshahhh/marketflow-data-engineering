-- Quarantine a single record rejected before it reached Snowflake
-- (malformed API payload). Bulk rejections are handled in SQL instead.
INSERT INTO AUDIT.DATA_QUALITY_ERRORS
    (RUN_ID, BATCH_ID, TABLE_NAME, SYMBOL, TRADE_DATE, ERROR_TYPE, ERROR_MESSAGE, RAW_RECORD, FAILED_AT)
SELECT
    %(run_id)s,
    %(batch_id)s,
    %(table_name)s,
    %(symbol)s,
    TRY_TO_DATE(%(trade_date)s::VARCHAR),
    %(error_type)s,
    %(error_message)s,
    PARSE_JSON(%(raw_record)s),
    CURRENT_TIMESTAMP();
