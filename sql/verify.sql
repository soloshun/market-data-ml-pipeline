-- Run after each full fixture load. Expect 30 rows and 30 distinct keys per table.
SELECT 'market_prices' AS dataset, count(*) AS rows,
       count(DISTINCT (symbol, trade_date)) AS distinct_keys
FROM market_data.market_prices WHERE symbol = 'XAUUSD-DEMO'
UNION ALL
SELECT 'market_features', count(*), count(DISTINCT (symbol, trade_date))
FROM market_data.market_features WHERE symbol = 'XAUUSD-DEMO';

SELECT p.trade_date, p.close, f.daily_return, f.sma_5, f.sma_20, f.volatility_5
FROM market_data.market_prices AS p
JOIN market_data.market_features AS f USING (symbol, trade_date)
WHERE p.symbol = 'XAUUSD-DEMO'
ORDER BY p.trade_date;
