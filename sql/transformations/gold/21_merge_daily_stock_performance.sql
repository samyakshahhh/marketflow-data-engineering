-- Daily performance metrics.
--
-- Only symbols touched by the current Silver window are recalculated, and only
-- from their earliest changed trade date onwards. `lookback_days` of extra
-- history is read as well so the 30 day windows are already full at the first
-- recalculated row; that extra history feeds the window functions but is not
-- written back.
--
-- Returns, moving averages and volatility are produced in a single pass over
-- the window rather than in three separate statements over the same rows.
MERGE INTO GOLD.DAILY_STOCK_PERFORMANCE t
USING (
    WITH changed AS (
        SELECT
            SYMBOL,
            MIN(TRADE_DATE) AS MIN_TRADE_DATE
        FROM SILVER.STOCK_PRICES
        WHERE LOADED_AT >  TO_TIMESTAMP_NTZ(%(from_ts)s)
          AND LOADED_AT <= TO_TIMESTAMP_NTZ(%(to_ts)s)
        GROUP BY SYMBOL
    ),
    scoped AS (
        SELECT
            p.SYMBOL,
            p.TRADE_DATE,
            p.CLOSE_PRICE,
            p.VOLUME,
            p.VWAP,
            c.MIN_TRADE_DATE
        FROM SILVER.STOCK_PRICES p
        JOIN changed c
          ON c.SYMBOL = p.SYMBOL
        WHERE p.TRADE_DATE >= DATEADD(day, %(lookback_days)s, c.MIN_TRADE_DATE)
    ),
    with_previous AS (
        SELECT
            scoped.*,
            LAG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE) AS PREV_CLOSE_PRICE,
            -- NULL until the window is complete, so a partial window is never
            -- published as if it were a full 7 or 30 day average.
            IFF(COUNT(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE
                                         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) = 7,
                AVG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE
                                       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
                NULL) AS MOVING_AVG_7D,
            IFF(COUNT(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE
                                         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 30,
                AVG(CLOSE_PRICE) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE
                                       ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
                NULL) AS MOVING_AVG_30D
        FROM scoped
    ),
    with_returns AS (
        SELECT
            with_previous.*,
            CLOSE_PRICE - PREV_CLOSE_PRICE                                       AS DAILY_RETURN,
            (CLOSE_PRICE - PREV_CLOSE_PRICE) / NULLIF(PREV_CLOSE_PRICE, 0) * 100 AS DAILY_RETURN_PERCENT
        FROM with_previous
    ),
    with_volatility AS (
        SELECT
            with_returns.*,
            IFF(COUNT(DAILY_RETURN_PERCENT) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE
                                                  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 30,
                STDDEV_SAMP(DAILY_RETURN_PERCENT) OVER (PARTITION BY SYMBOL ORDER BY TRADE_DATE
                                                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
                NULL) AS VOLATILITY_30D
        FROM with_returns
    )
    SELECT
        SYMBOL,
        TRADE_DATE,
        CAST(CLOSE_PRICE          AS NUMBER(18,6)) AS CLOSE_PRICE,
        CAST(PREV_CLOSE_PRICE     AS NUMBER(18,6)) AS PREV_CLOSE_PRICE,
        CAST(DAILY_RETURN         AS NUMBER(18,6)) AS DAILY_RETURN,
        CAST(DAILY_RETURN_PERCENT AS NUMBER(18,6)) AS DAILY_RETURN_PERCENT,
        CAST(MOVING_AVG_7D        AS NUMBER(18,6)) AS MOVING_AVG_7D,
        CAST(MOVING_AVG_30D       AS NUMBER(18,6)) AS MOVING_AVG_30D,
        CAST(VOLATILITY_30D       AS NUMBER(18,6)) AS VOLATILITY_30D,
        VOLUME,
        CAST(VWAP                 AS NUMBER(18,6)) AS VWAP
    FROM with_volatility
    WHERE TRADE_DATE >= MIN_TRADE_DATE
) s
   ON t.SYMBOL = s.SYMBOL
  AND t.TRADE_DATE = s.TRADE_DATE
WHEN MATCHED THEN UPDATE SET
    t.CLOSE_PRICE          = s.CLOSE_PRICE,
    t.PREV_CLOSE_PRICE     = s.PREV_CLOSE_PRICE,
    t.DAILY_RETURN         = s.DAILY_RETURN,
    t.DAILY_RETURN_PERCENT = s.DAILY_RETURN_PERCENT,
    t.MOVING_AVG_7D        = s.MOVING_AVG_7D,
    t.MOVING_AVG_30D       = s.MOVING_AVG_30D,
    t.VOLATILITY_30D       = s.VOLATILITY_30D,
    t.VOLUME               = s.VOLUME,
    t.VWAP                 = s.VWAP,
    t.BATCH_ID             = %(batch_id)s,
    t.RUN_ID               = %(run_id)s,
    t.CALCULATED_AT        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    SYMBOL, TRADE_DATE, CLOSE_PRICE, PREV_CLOSE_PRICE, DAILY_RETURN, DAILY_RETURN_PERCENT,
    MOVING_AVG_7D, MOVING_AVG_30D, VOLATILITY_30D, VOLUME, VWAP, BATCH_ID, RUN_ID, CALCULATED_AT
) VALUES (
    s.SYMBOL, s.TRADE_DATE, s.CLOSE_PRICE, s.PREV_CLOSE_PRICE, s.DAILY_RETURN, s.DAILY_RETURN_PERCENT,
    s.MOVING_AVG_7D, s.MOVING_AVG_30D, s.VOLATILITY_30D, s.VOLUME, s.VWAP,
    %(batch_id)s, %(run_id)s, CURRENT_TIMESTAMP()
);
