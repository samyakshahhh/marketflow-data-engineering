-- Append API company profile records to Bronze.
-- See insert_stock_prices_raw.sql for how the last line is expanded.
INSERT INTO BRONZE.COMPANY_PROFILE_RAW
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
