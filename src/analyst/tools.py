"""Tool functions registered with Gemini. All return formatted strings."""
import duckdb
import pandas as pd
from config import DB_PATH


def _fmt(df: pd.DataFrame, empty: str = "No rows.") -> str:
    if df is None or df.empty:
        return empty
    return df.to_string(index=False)


def get_price_history(symbol: str, days: int = 30) -> str:
    """
    Daily price + technical signals for `symbol`, most recent `days` trading days.
    Columns: date, close, volume, rsi_14, adx_14, atr_14, macd_hist, sma_50,
    sma_150, sma_200, rs_rank, pct_from_52w_high, volume_ratio_20d.
    Use to read the daily setup: base, breakout, volume pattern, MA stack.
    Note: rs_rank is only authoritative on the latest row; older rows show 50.

    Args:
        symbol: NSE ticker without suffix, e.g. "RELIANCE".
        days:   number of recent sessions (default 30, capped at 400).
    """
    days = max(1, min(int(days), 400))
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute(f"""
            SELECT s.date, p.close, p.volume,
                   s.rsi_14, s.adx_14, s.atr_14, s.macd_hist,
                   s.sma_50, s.sma_150, s.sma_200,
                   s.rs_rank, s.pct_from_52w_high, s.volume_ratio_20d
            FROM signals s
            JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
            WHERE s.symbol = ?
            ORDER BY s.date DESC
            LIMIT {days}
        """, (symbol,)).fetchdf()
    return f"--- {symbol} daily, last {len(df)} sessions ---\n{_fmt(df, 'no data')}"


def get_weekly_history(symbol: str, weeks: int = 10) -> str:
    """
    Weekly OHLCV candles for `symbol`. Use to confirm weekly Stage-2:
    higher highs/lows, up-week volume expansion.

    Args:
        symbol: NSE ticker without suffix.
        weeks:  number of recent weeks (default 10, capped at 104).
    """
    weeks = max(1, min(int(weeks), 104))
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute(f"""
            SELECT date, open, high, low, close, volume
            FROM weekly_prices WHERE symbol = ?
            ORDER BY date DESC LIMIT {weeks}
        """, (symbol,)).fetchdf()
    return f"--- {symbol} weekly, last {len(df)} weeks ---\n{_fmt(df, 'no weekly data')}"


def get_fundamentals(symbol: str) -> str:
    """
    Latest scraped fundamentals for `symbol`: EPS, EPS growth YoY, revenue,
    revenue growth YoY, earnings surprise, promoter holding, fetch date.
    Check whether a technical setup is backed by healthy earnings and stable
    promoter ownership.

    Args:
        symbol: NSE ticker without suffix.
    """
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute("""
            SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy,
                   earnings_surprise, promoter_holding, fetch_date
            FROM fundamentals WHERE symbol = ?
            ORDER BY fetch_date DESC LIMIT 1
        """, (symbol,)).fetchdf()
    return _fmt(df, f"no stored fundamentals for {symbol}")


def get_news(symbol: str, days: int = 14) -> str:
    """
    Stored news sentiment for `symbol` in the last `days` days. For fresher
    or material news beyond local data, use search_web instead.

    Args:
        symbol: NSE ticker without suffix.
        days:   lookback window in days (default 14, capped at 90).
    """
    days = max(1, min(int(days), 90))
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute(f"""
            SELECT date, sentiment_score, material_event, summary
            FROM news WHERE symbol = ?
              AND date > current_date - INTERVAL {days} DAY
            ORDER BY date DESC
        """, (symbol,)).fetchdf()
    return _fmt(df, f"no stored news for {symbol} in last {days} days")


def get_research_notes(symbol: str = "", days: int = 45) -> str:
    """
    Your own prior research notes from the research_journal table.
    Pass symbol="" to get a summary of all recently tracked symbols.
    Pass a specific symbol to get full note detail for that stock.

    Args:
        symbol: NSE ticker without suffix, or "" for all recent notes.
        days:   lookback window in days (default 45, capped at 365).
    """
    days = max(1, min(int(days), 365))
    with duckdb.connect(DB_PATH, read_only=True) as c:
        if symbol:
            df = c.execute(f"""
                SELECT symbol, date, conviction, status, entry_trigger, thesis, risk_factors
                FROM research_journal
                WHERE symbol = ? AND date > current_date - INTERVAL {days} DAY
                ORDER BY date DESC
            """, (symbol,)).fetchdf()
        else:
            df = c.execute(f"""
                SELECT symbol, date, conviction, status, entry_trigger
                FROM research_journal
                WHERE date > current_date - INTERVAL {days} DAY
                ORDER BY date DESC
            """).fetchdf()
    return _fmt(df, "no research notes found")


def get_open_position_detail(symbol: str = "") -> str:
    """
    Open positions with current context: entry price, current close, PnL%,
    stop loss, target, latest RSI and ADX.
    Pass symbol="" for all open positions, or a specific symbol to drill in.

    Args:
        symbol: NSE ticker without suffix, or "" for all open positions.
    """
    where = "WHERE p.status = 'OPEN'"
    args: tuple = ()
    if symbol:
        where += " AND p.symbol = ?"
        args = (symbol,)
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute(f"""
            SELECT p.symbol, p.entry_date, p.entry_price, p.quantity,
                   p.stop_loss, p.target, p.position_pct, p.thesis_summary,
                   pr.close AS current_close, s.rsi_14, s.adx_14,
                   round((pr.close - p.entry_price) / p.entry_price * 100, 2) AS pnl_pct
            FROM portfolio p
            LEFT JOIN signals s ON p.symbol = s.symbol
            LEFT JOIN prices  pr ON p.symbol = pr.symbol AND s.date = pr.date
            {where}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY s.date DESC) = 1
        """, args).fetchdf()
    return _fmt(df, "no open positions")


def get_position_history(symbol: str) -> str:
    """
    For an open position, returns two windows:
    1. ±15 trading days around entry_date — to recall the original setup.
    2. Latest 15 daily rows — to see current chart state.
    Use when reviewing whether an open position's thesis is still intact.

    Args:
        symbol: NSE ticker without suffix. Must be an open position.
    """
    with duckdb.connect(DB_PATH, read_only=True) as c:
        row = c.execute(
            "SELECT entry_date FROM portfolio WHERE symbol = ? AND status='OPEN' LIMIT 1",
            (symbol,)
        ).fetchone()
        if not row:
            return f"no open position found for {symbol}"
        entry = row[0]
        around = c.execute("""
            SELECT s.date, p.close, p.volume, s.rsi_14, s.adx_14, s.sma_50, s.sma_200
            FROM signals s JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
            WHERE s.symbol = ?
              AND s.date BETWEEN CAST(? AS DATE) - INTERVAL 15 DAY
                             AND CAST(? AS DATE) + INTERVAL 15 DAY
            ORDER BY s.date
        """, (symbol, entry, entry)).fetchdf()
        latest = c.execute("""
            SELECT s.date, p.close, p.volume, s.rsi_14, s.adx_14, s.sma_50, s.sma_200
            FROM signals s JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
            WHERE s.symbol = ?
            ORDER BY s.date DESC LIMIT 15
        """, (symbol,)).fetchdf()
    return (
        f"--- {symbol} around entry ({entry}) ---\n{_fmt(around)}\n\n"
        f"--- {symbol} latest 15 sessions ---\n{_fmt(latest)}"
    )


def execute_read_only_query(query: str) -> str:
    """
    Escape hatch: execute a raw SELECT on the DuckDB database.
    Only SELECT and WITH...SELECT statements are permitted.
    Prefer the dedicated tools above; use this for aggregations, peer
    comparisons, or custom joins they don't cover.
    The full schema is documented in the system prompt.

    Args:
        query: A SQL SELECT statement.
    """
    q = query.strip()
    upper = q.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return "Error: only SELECT / WITH...SELECT allowed."
    forbidden = ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
                 "CREATE", "REPLACE", "TRUNCATE")
    padded = f" {upper} "
    if any(f" {k} " in padded for k in forbidden):
        return "Error: write/DDL keyword detected. Query rejected."
    try:
        with duckdb.connect(DB_PATH, read_only=True) as c:
            df = c.execute(q).fetchdf()
        return _fmt(df)
    except Exception as e:
        return f"Error executing query: {e}"
