-- Trailing window summary per symbol: 7D, 30D and 90D.
--
-- The table holds one row per symbol and period, "as of the latest trade date
-- available", so it is recomputed in full and merged on (SYMBOL, PERIOD):
-- cheap at portfolio scale and deterministic on every rerun.
MERGE INTO GOLD.STOCK_PERFORMANCE_SUMMARY t
USING (
    WITH latest AS (
        SELECT
            SYMBOL,
            MAX(TRADE_DATE) AS LATEST_TRADE_DATE
        FROM GOLD.DAILY_STOCK_PERFORMANCE
        GROUP BY SYMBOL
    ),
    periods AS (
        SELECT '7D' AS PERIOD, 7 AS WINDOW_DAYS
        UNION ALL
        SELECT '30D', 30
        UNION ALL
        SELECT '90D', 90
    ),
    scoped AS (
        SELECT
            d.SYMBOL,
            p.PERIOD,
            d.TRADE_DATE,
            d.CLOSE_PRICE,
            d.VOLUME,
            d.DAILY_RETURN_PERCENT
        FROM GOLD.DAILY_STOCK_PERFORMANCE d
        JOIN latest l
          ON l.SYMBOL = d.SYMBOL
        CROSS JOIN periods p
        WHERE d.TRADE_DATE > DATEADD(day, -p.WINDOW_DAYS, l.LATEST_TRADE_DATE)
    )
    SELECT
        SYMBOL,
        PERIOD,
        MIN(TRADE_DATE)                                         AS START_DATE,
        MAX(TRADE_DATE)                                         AS END_DATE,
        CAST(MIN_BY(CLOSE_PRICE, TRADE_DATE) AS NUMBER(18,6))   AS START_PRICE,
        CAST(MAX_BY(CLOSE_PRICE, TRADE_DATE) AS NUMBER(18,6))   AS END_PRICE,
        CAST((MAX_BY(CLOSE_PRICE, TRADE_DATE) - MIN_BY(CLOSE_PRICE, TRADE_DATE))
             / NULLIF(MIN_BY(CLOSE_PRICE, TRADE_DATE), 0) * 100
             AS NUMBER(18,6))                                   AS TOTAL_RETURN,
        CAST(AVG(DAILY_RETURN_PERCENT) AS NUMBER(18,6))         AS AVG_DAILY_RETURN,
        CAST(AVG(VOLUME) AS NUMBER(20,0))                       AS AVG_VOLUME,
        CAST(MAX(CLOSE_PRICE) AS NUMBER(18,6))                  AS MAX_PRICE,
        CAST(MIN(CLOSE_PRICE) AS NUMBER(18,6))                  AS MIN_PRICE,
        CAST(STDDEV_SAMP(DAILY_RETURN_PERCENT) AS NUMBER(18,6)) AS VOLATILITY,
        COUNT(*)                                                AS TRADING_DAYS
    FROM scoped
    GROUP BY SYMBOL, PERIOD
) s
   ON t.SYMBOL = s.SYMBOL
  AND t.PERIOD = s.PERIOD
WHEN MATCHED THEN UPDATE SET
    t.START_DATE       = s.START_DATE,
    t.END_DATE         = s.END_DATE,
    t.START_PRICE      = s.START_PRICE,
    t.END_PRICE        = s.END_PRICE,
    t.TOTAL_RETURN     = s.TOTAL_RETURN,
    t.AVG_DAILY_RETURN = s.AVG_DAILY_RETURN,
    t.AVG_VOLUME       = s.AVG_VOLUME,
    t.MAX_PRICE        = s.MAX_PRICE,
    t.MIN_PRICE        = s.MIN_PRICE,
    t.VOLATILITY       = s.VOLATILITY,
    t.TRADING_DAYS     = s.TRADING_DAYS,
    t.BATCH_ID         = %(batch_id)s,
    t.RUN_ID           = %(run_id)s,
    t.CALCULATED_AT    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    SYMBOL, PERIOD, START_DATE, END_DATE, START_PRICE, END_PRICE, TOTAL_RETURN,
    AVG_DAILY_RETURN, AVG_VOLUME, MAX_PRICE, MIN_PRICE, VOLATILITY, TRADING_DAYS,
    BATCH_ID, RUN_ID, CALCULATED_AT
) VALUES (
    s.SYMBOL, s.PERIOD, s.START_DATE, s.END_DATE, s.START_PRICE, s.END_PRICE, s.TOTAL_RETURN,
    s.AVG_DAILY_RETURN, s.AVG_VOLUME, s.MAX_PRICE, s.MIN_PRICE, s.VOLATILITY, s.TRADING_DAYS,
    %(batch_id)s, %(run_id)s, CURRENT_TIMESTAMP()
);
