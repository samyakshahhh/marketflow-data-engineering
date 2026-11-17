-- Append API price records to Bronze.
--
-- The placeholder on the last line is expanded by src/warehouse/loader.py into
-- one bind group per record. Only the placeholders are generated: the values
-- themselves are always bound, never formatted into the statement.
--
-- Comments in this file must not contain a percent sign - the Snowflake
-- connector binds parameters across the whole statement text, comments
-- included.
INSERT INTO BRONZE.STOCK_PRICES_RAW
    (RAW_DATA, SYMBOL, SOURCE_SYSTEM, API_ENDPOINT, BATCH_ID, RUN_ID, INGESTED_AT)
SELECT
    PARSE_JSON(column1),
    column2,
    column3,
    column4,
    column5,
    column6,
    CURRENT_TIMESTAMP()
FROM VALUES
{value_rows}
