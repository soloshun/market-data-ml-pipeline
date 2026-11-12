BEGIN;

CREATE TABLE IF NOT EXISTS market_prices (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL NOT NULL CHECK (open > 0 AND open < 1e999),
    high REAL NOT NULL CHECK (high > 0 AND high < 1e999),
    low REAL NOT NULL CHECK (low > 0 AND low < 1e999),
    close REAL NOT NULL CHECK (close > 0 AND close < 1e999),
    volume INTEGER NOT NULL CHECK (volume >= 0),
    source TEXT NOT NULL CHECK (source IN ('fixture', 'yfinance')),
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_date),
    CHECK (low <= min(open, close) AND high >= max(open, close)),
    CHECK ((source = 'fixture') = (symbol = 'XAUUSD-DEMO'))
);

CREATE TABLE IF NOT EXISTS market_features (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    daily_return REAL,
    sma_5 REAL,
    sma_20 REAL,
    volatility_5 REAL,
    computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_date),
    FOREIGN KEY (symbol, trade_date) REFERENCES market_prices (symbol, trade_date)
);

COMMIT;
