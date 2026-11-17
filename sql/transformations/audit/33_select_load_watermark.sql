-- Current watermark for one layer/table/symbol. No row means "never loaded".
SELECT
    LAST_LOADED_DATE,
    LAST_PROCESSED_TS,
    LAST_RUN_ID,
    LAST_BATCH_ID,
    UPDATED_AT
FROM AUDIT.LOAD_WATERMARK
WHERE LAYER = %(layer)s
  AND TABLE_NAME = %(table_name)s
  AND SYMBOL = %(symbol)s;
