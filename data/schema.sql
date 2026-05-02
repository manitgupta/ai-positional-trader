-- Universe table
CREATE TABLE IF NOT EXISTS universe (
    symbol VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    series VARCHAR,
    sector VARCHAR,
    industry VARCHAR
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

-- Weekly price table
CREATE TABLE IF NOT EXISTS weekly_prices (
    symbol       VARCHAR,
    date         DATE,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    volume       BIGINT,
    PRIMARY KEY (symbol, date)
);

-- Weekly signals table
CREATE TABLE IF NOT EXISTS weekly_signals (
    symbol          VARCHAR,
    date            DATE,
    sma_10          DOUBLE,
    sma_30          DOUBLE,
    rsi_14          DOUBLE,
    volume_ratio_10w DOUBLE,
    mansfield_rs    DOUBLE,
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
    raw_momentum_12m DOUBLE,     -- 12 month raw percentage return
    pct_from_52w_high DOUBLE,
    volume_ratio_20d  DOUBLE,    -- today's vol / 20d avg
    bb_width        DOUBLE,
    daily_rs        DOUBLE,
    PRIMARY KEY (symbol, date)
);

-- Annual results
CREATE TABLE IF NOT EXISTS annual_results (
    symbol          VARCHAR,
    quarter         VARCHAR,
    eps             DOUBLE,
    eps_growth_yoy  DOUBLE,
    revenue         DOUBLE,
    rev_growth_yoy  DOUBLE,
    earnings_surprise DOUBLE,
    roe             DOUBLE,
    debt_to_equity  DOUBLE,
    promoter_holding DOUBLE,
    fetch_date      DATE,
    PRIMARY KEY (symbol, quarter)
);

-- Quarterly results
CREATE TABLE IF NOT EXISTS quarterly_results (
    symbol          VARCHAR,
    quarter         VARCHAR,
    eps             DOUBLE,
    eps_growth_yoy  DOUBLE,
    revenue         DOUBLE,
    rev_growth_yoy  DOUBLE,
    net_profit      DOUBLE,
    fetch_date      DATE,
    promoter_holding DOUBLE,
    fii_holding     DOUBLE,
    dii_holding     DOUBLE,
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
    status          VARCHAR,     -- OPEN | CLOSED
    exit_date       DATE,
    exit_price      DOUBLE
);

-- Account state for capital tracking
CREATE TABLE IF NOT EXISTS account (
    id INTEGER PRIMARY KEY,
    total_capital DOUBLE,
    updated_at TIMESTAMP
);

-- Seed initial capital if not exists
INSERT INTO account (id, total_capital, updated_at)
SELECT 1, 1000000.0, current_timestamp
WHERE NOT EXISTS (SELECT 1 FROM account WHERE id = 1);

-- Delivery volume stats fetched from NSE archive files
CREATE TABLE IF NOT EXISTS delivery_data (
    symbol          VARCHAR,
    date            DATE,
    traded_qty      BIGINT,
    deliverable_qty BIGINT,
    delivery_pct    DOUBLE,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS symbol_state (
    symbol                  VARCHAR PRIMARY KEY,
    first_seen_date         DATE,        -- first time agent wrote about it
    last_seen_date          DATE,        -- most recent review
    days_tracked            INTEGER,     -- (last_seen - first_seen) in calendar days
    current_status          VARCHAR,     -- WATCHING | TRIGGERED | ENTERED | EXITED | REJECTED | STALE
    current_conviction      INTEGER,     -- latest score 1-10
    current_thesis          VARCHAR,     -- latest one-paragraph view
    current_entry_trigger   VARCHAR,
    current_stop_loss       DOUBLE,
    current_target          DOUBLE,
    conviction_history      VARCHAR,     -- JSON: [{"date": "2026-04-18", "conviction": 7}, ...] capped at 10 most recent
    status_history          VARCHAR,     -- JSON: [{"date": "...", "status": "WATCHING", "reason": "..."}, ...] capped at 10
    trigger_history         VARCHAR,     -- JSON: [{"date": "...", "entry_trigger": "..."}] capped at 5
    trigger_static_days     INTEGER,     -- calendar days the entry_trigger string has been unchanged
    rejection_reason        VARCHAR,     -- only set when current_status = REJECTED
    last_action             VARCHAR,     -- raw action emitted last run (e.g., "WATCH_FOR_ENTRY")
    last_run_date           DATE,
    updated_at              TIMESTAMP
);

