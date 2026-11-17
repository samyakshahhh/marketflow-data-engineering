-- Record how far a layer has successfully processed. Written only after the
-- step succeeded, so a failed or partial run never advances the pipeline.
MERGE INTO AUDIT.LOAD_WATERMARK t
USING (
    SELECT
        %(layer)s::VARCHAR      AS LAYER,
        %(table_name)s::VARCHAR AS TABLE_NAME,
        %(symbol)s::VARCHAR     AS SYMBOL
) s
   ON t.LAYER = s.LAYER
  AND t.TABLE_NAME = s.TABLE_NAME
  AND t.SYMBOL = s.SYMBOL
WHEN MATCHED THEN UPDATE SET
    t.LAST_LOADED_DATE  = COALESCE(TRY_TO_DATE(%(last_loaded_date)s::VARCHAR), t.LAST_LOADED_DATE),
    t.LAST_PROCESSED_TS = COALESCE(TRY_TO_TIMESTAMP_NTZ(%(last_processed_ts)s::VARCHAR), t.LAST_PROCESSED_TS),
    t.LAST_RUN_ID       = %(run_id)s,
    t.LAST_BATCH_ID     = %(batch_id)s,
    t.UPDATED_AT        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    LAYER, TABLE_NAME, SYMBOL, LAST_LOADED_DATE, LAST_PROCESSED_TS,
    LAST_RUN_ID, LAST_BATCH_ID, UPDATED_AT
) VALUES (
    s.LAYER, s.TABLE_NAME, s.SYMBOL,
    TRY_TO_DATE(%(last_loaded_date)s::VARCHAR),
    TRY_TO_TIMESTAMP_NTZ(%(last_processed_ts)s::VARCHAR),
    %(run_id)s, %(batch_id)s, CURRENT_TIMESTAMP()
);
