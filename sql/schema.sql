BEGIN;

-- A dedicated schema keeps these tables outside Supabase's default public Data API.
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.market_prices (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    open double precision NOT NULL CHECK (open > 0 AND open < 'Infinity'::double precision),
    high double precision NOT NULL CHECK (high > 0 AND high < 'Infinity'::double precision),
    low double precision NOT NULL CHECK (low > 0 AND low < 'Infinity'::double precision),
    close double precision NOT NULL CHECK (close > 0 AND close < 'Infinity'::double precision),
    volume bigint NOT NULL CHECK (volume >= 0),
    source text NOT NULL CHECK (source IN ('fixture', 'yfinance')),
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date),
    CHECK (low <= least(open, close) AND high >= greatest(open, close)),
    CHECK ((source = 'fixture') = (symbol = 'XAUUSD-DEMO'))
);

CREATE TABLE IF NOT EXISTS market_data.market_features (
    symbol text NOT NULL,
    trade_date date NOT NULL,
    daily_return double precision,
    sma_5 double precision,
    sma_20 double precision,
    volatility_5 double precision,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date),
    FOREIGN KEY (symbol, trade_date)
        REFERENCES market_data.market_prices (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS market_data.hourly_prices (
    symbol text NOT NULL,
    bar_start_utc timestamptz NOT NULL,
    open double precision NOT NULL CHECK (open > 0 AND open < 'Infinity'::double precision),
    high double precision NOT NULL CHECK (high > 0 AND high < 'Infinity'::double precision),
    low double precision NOT NULL CHECK (low > 0 AND low < 'Infinity'::double precision),
    close double precision NOT NULL CHECK (close > 0 AND close < 'Infinity'::double precision),
    volume bigint NOT NULL CHECK (volume >= 0),
    source text NOT NULL CHECK (source IN ('fixture', 'yfinance')),
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, bar_start_utc),
    CHECK (low <= least(open, close) AND high >= greatest(open, close)),
    CHECK ((source = 'fixture') = (symbol = 'XAUUSD-DEMO'))
);

CREATE TABLE IF NOT EXISTS market_data.hourly_features (
    symbol text NOT NULL,
    bar_start_utc timestamptz NOT NULL,
    hourly_return double precision,
    sma_5 double precision,
    sma_20 double precision,
    volatility_5 double precision,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, bar_start_utc),
    FOREIGN KEY (symbol, bar_start_utc)
        REFERENCES market_data.hourly_prices (symbol, bar_start_utc)
);

COMMIT;
