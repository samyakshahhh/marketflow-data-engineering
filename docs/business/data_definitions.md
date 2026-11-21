# Data definitions

Every definition below was extracted from the existing implementation on
2026-11-21. The code is the source of truth; if code and this page disagree,
flag it and treat the code as current until a Jira ticket decides otherwise.

## Source system

| Item | Value | Source |
| --- | --- | --- |
| Provider | Financial Modeling Prep (FMP), `SOURCE_SYSTEM = 'FMP'` | `config/pipeline.yaml` |
| Base URL | `https://financialmodelingprep.com/stable` (legacy `/api/v3` shape also accepted) | `config/pipeline.yaml`, `src/api/fmp_client.py` |
| End-of-day prices | endpoint `historical-price-eod/full`, params `symbol`, `from`, `to` | `src/api/fmp_client.py` |
| Company profile | endpoint `profile`, param `symbol` | `src/api/fmp_client.py` |
| Universe | Enabled symbols in `config/symbols.yaml` (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA), overridable by `MARKETFLOW_SYMBOLS` | `config/symbols.yaml`, `src/utils/config.py` |
| History on first load | `initial_lookback_days` = 730 calendar days | `config/pipeline.yaml` |
| Late-correction overlap | `incremental_overlap_days` = 3 days before the Bronze watermark | `config/pipeline.yaml` |

## Entities and grain

| Entity | Grain | Table |
| --- | --- | --- |
| Daily stock price | one row per `SYMBOL` + `TRADE_DATE` | `SILVER.STOCK_PRICES` |
| Company profile | one row per `SYMBOL` (latest received profile wins) | `SILVER.COMPANY_PROFILE` |
| Daily performance | one row per `SYMBOL` + `TRADE_DATE` | `GOLD.DAILY_STOCK_PERFORMANCE` |
| Performance summary | one row per `SYMBOL` + `PERIOD` (`7D`, `30D`, `90D`) | `GOLD.STOCK_PERFORMANCE_SUMMARY` |

## Price fields (Silver)

Taken as reported by FMP, parsed from the raw payload in `SILVER.VW_STOCK_PRICES_STAGING` (`sql/07_create_views.sql`).

| Column | Payload key | Meaning |
| --- | --- | --- |
| `SYMBOL` | `symbol` (fallback: requested symbol), upper-cased | Ticker |
| `TRADE_DATE` | `date` | Trading day of the end-of-day record |
| `OPEN_PRICE`, `HIGH_PRICE`, `LOW_PRICE`, `CLOSE_PRICE` | `open`, `high`, `low`, `close` | Provider OHLC |
| `VOLUME` | `volume` | Shares traded |
| `CHANGE_AMOUNT`, `CHANGE_PERCENT` | `change`, `changePercent` | Provider-reported change (not recalculated) |
| `VWAP` | `vwap` | Provider-reported volume-weighted average price |

Prices are stored in the currency the provider returns; no conversion is applied.

## Company profile fields (Silver)

From `SILVER.VW_COMPANY_PROFILE_STAGING`: `COMPANY_NAME` ← `companyName`,
`INDUSTRY` ← `industry`, `SECTOR` ← `sector`, `EXCHANGE` ← `exchangeFullName`
(fallback `exchange`), `CURRENCY` ← `currency`, `COUNTRY` ← `country`,
`MARKET_CAP` ← `marketCap`.

## Calculations (Gold)

Source: `sql/transformations/gold/21_merge_daily_stock_performance.sql`,
`22_merge_stock_performance_summary.sql`. Windows are over **trading-day rows**
present in Silver, per symbol, ordered by `TRADE_DATE`.

| Measure | Definition |
| --- | --- |
| `PREV_CLOSE_PRICE` | Close of the previous available trading row for the symbol; NULL for the first row |
| `DAILY_RETURN` | `CLOSE_PRICE − PREV_CLOSE_PRICE` |
| `DAILY_RETURN_PERCENT` | `(CLOSE_PRICE − PREV_CLOSE_PRICE) / PREV_CLOSE_PRICE × 100`; NULL when previous close is NULL or 0 |
| `MOVING_AVG_7D` | Mean close over the current and 6 preceding rows; NULL until 7 rows exist |
| `MOVING_AVG_30D` | Mean close over the current and 29 preceding rows; NULL until 30 rows exist |
| `VOLATILITY_30D` | Sample standard deviation of `DAILY_RETURN_PERCENT` over the current and 29 preceding rows; NULL until 30 non-null returns exist; **not annualised** |

Summary (`PERIOD` = 7D / 30D / 90D), as of each symbol's latest trade date in Gold;
window = rows with `TRADE_DATE > latest − N calendar days`:

| Measure | Definition |
| --- | --- |
| `START_DATE` / `END_DATE` | First / last trade date in the window |
| `START_PRICE` / `END_PRICE` | Close on `START_DATE` / `END_DATE` |
| `TOTAL_RETURN` | `(END_PRICE − START_PRICE) / START_PRICE × 100` (NULL if start price is 0) |
| `AVG_DAILY_RETURN` | Mean `DAILY_RETURN_PERCENT` in the window |
| `AVG_VOLUME` | Mean volume |
| `MAX_PRICE` / `MIN_PRICE` | Highest / lowest close |
| `VOLATILITY` | Sample std. dev. of `DAILY_RETURN_PERCENT` in the window |
| `TRADING_DAYS` | Number of rows in the window |

## Data-quality rules

| Level | Rule | Error type | Source |
| --- | --- | --- | --- |
| Python, before Bronze | Record is an object; `date` present and ISO-parseable; `close` present and numeric | `INVALID_API_RECORD` | `src/validation/data_quality.py` |
| Bronze → Silver | `SYMBOL`, `TRADE_DATE`, `CLOSE_PRICE` not null | `NULL_REQUIRED_FIELD` | `sql/07_create_views.sql` |
| | Trade date parseable, not in the future, not before 1990-01-01 | `INVALID_DATE` | same |
| | `CLOSE > 0`; `HIGH ≥ LOW, OPEN, CLOSE`; `LOW ≤ OPEN, CLOSE` (comparisons with a missing optional price are not failures) | `INVALID_PRICE` | same |
| | `VOLUME` present and ≥ 0 | `INVALID_VOLUME` | same |
| | Same `SYMBOL` + `TRADE_DATE` in a window: most recently ingested record wins, others quarantined | `DUPLICATE_RECORD` | `sql/transformations/silver/12_…`, `13_…` |

Rejected records are quarantined in `AUDIT.DATA_QUALITY_ERRORS` with the raw
payload, never deleted. Failing checks are warnings, not task failures.

## Stated assumptions (from README *Design decisions*)

- "Today" is UTC.
- 7/30/90-day summary windows are calendar days over available trading days.
- Volatility is the sample standard deviation of daily percentage returns, not annualised.
- The FMP `/stable` EOD endpoint returns the fields listed above.
