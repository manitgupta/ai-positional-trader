-- Universe table
CREATE TABLE IF NOT EXISTS universe (
    symbol VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    series VARCHAR
);

-- Core price table (partitioned by date)
CREATE TABLE IF NOT EXISTS prices (
    symbol       VARCHAR,
    date         DATE,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       BIGINT,
    PRIMARY KEY (symbol, date)
);

-- Derived technical signals (recomputed nightly)
CREATE TABLE IF NOT EXISTS signals (
    symbol          VARCHAR,
    date            DATE,
    rsi_14          DOUBLE,
    adx_14          DOUBLE,
    atr_14          DOUBLE,
    macd_hist       DOUBLE,
    sma_50          DOUBLE,
    sma_150         DOUBLE,
    sma_200         DOUBLE,
    above_200ma     BOOLEAN,
    rs_rank         INTEGER,     -- percentile vs Nifty 50, 0-100
    pct_from_52w_high DOUBLE,
    volume_ratio_20d  DOUBLE,    -- today's vol / 20d avg
    PRIMARY KEY (symbol, date)
);

-- Quarterly fundamentals (updated on results season)
CREATE TABLE IF NOT EXISTS fundamentals (
    symbol          VARCHAR,
    quarter         VARCHAR,     -- e.g. Q3FY25
    eps             DOUBLE,
    eps_growth_yoy  DOUBLE,
    revenue         DOUBLE,
    rev_growth_yoy  DOUBLE,
    earnings_surprise DOUBLE,    -- % beat/miss vs estimate
    roe             DOUBLE,
    debt_to_equity  DOUBLE,
    promoter_holding DOUBLE,
    PRIMARY KEY (symbol, quarter)
);

-- Nightly news/sentiment per stock
CREATE TABLE IF NOT EXISTS news (
    symbol          VARCHAR,
    date            DATE,
    sentiment_score DOUBLE,      -- -1 to +1
    material_event  BOOLEAN,
    summary         VARCHAR,
    PRIMARY KEY (symbol, date)
);

-- Analyst's own research notes (persistent memory)
CREATE TABLE IF NOT EXISTS research_journal (
    id              INTEGER PRIMARY KEY,
    symbol          VARCHAR,
    date            DATE,
    thesis          VARCHAR,     -- full written rationale
    conviction      INTEGER,     -- 1-10
    status          VARCHAR,     -- WATCHING | ENTERED | EXITED | REJECTED
    entry_trigger   VARCHAR,     -- what would make the analyst enter
    risk_factors    VARCHAR
);

-- Portfolio state
CREATE TABLE IF NOT EXISTS portfolio (
    symbol          VARCHAR PRIMARY KEY,
    entry_date      DATE,
    entry_price     DOUBLE,
    quantity        INTEGER,
    stop_loss       DOUBLE,
    target          DOUBLE,
    position_pct    DOUBLE,      -- % of portfolio
    thesis_summary  VARCHAR,
    status          VARCHAR      -- OPEN | CLOSED
);
