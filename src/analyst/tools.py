"""Tool functions registered with Gemini. All return formatted strings."""
import datetime
import duckdb
import pandas as pd
import yfinance as yf
from config import DB_PATH

def get_macro_snapshot() -> str:
    print(f"🔧 [TOOL CALL] get_macro_snapshot")
    """
    Fetches a snapshot of key macro indicators:
    - Nifty 50 (^NSEI) position vs 50/200 DMA
    - India VIX (^INDIAVIX)
    - USD/INR (USDINR=X)
    - Brent Crude (BZ=F)
    - US 10Y (^TNX)
    Use to assess market environment risk.
    """
    try:
        # Nifty 50
        nifty = yf.Ticker("^NSEI").history(period="1y")
        if not nifty.empty:
            latest_close = nifty['Close'].iloc[-1]
            sma_50 = nifty['Close'].rolling(50).mean().iloc[-1]
            sma_200 = nifty['Close'].rolling(200).mean().iloc[-1]
            prev_close = nifty['Close'].iloc[-2]
            pct_change = (latest_close - prev_close) / prev_close * 100
            
            nifty_text = (
                f"Nifty 50: {latest_close:,.2f} ({pct_change:+.2f}%) | "
                f"vs 50MA: {((latest_close/sma_50)-1)*100:+.2f}% | "
                f"vs 200MA: {((latest_close/sma_200)-1)*100:+.2f}%"
            )
        else:
            nifty_text = "Nifty 50: No data"

        # India VIX
        vix = yf.Ticker("^INDIAVIX").history(period="5d")
        vix_val = vix['Close'].iloc[-1] if not vix.empty else "N/A"
        
        # USD/INR
        usdinr = yf.Ticker("USDINR=X").history(period="5d")
        usdinr_val = usdinr['Close'].iloc[-1] if not usdinr.empty else "N/A"
        
        # Brent Crude
        brent = yf.Ticker("BZ=F").history(period="5d")
        brent_val = brent['Close'].iloc[-1] if not brent.empty else "N/A"
        
        # US 10Y
        us10y = yf.Ticker("^TNX").history(period="5d")
        us10y_val = us10y['Close'].iloc[-1] if not us10y.empty else "N/A"
        
        vix_str = f"{vix_val:.2f}" if isinstance(vix_val, float) else str(vix_val)
        usdinr_str = f"{usdinr_val:.2f}" if isinstance(usdinr_val, float) else str(usdinr_val)
        brent_str = f"{brent_val:.2f}" if isinstance(brent_val, float) else str(brent_val)
        us10y_str = f"{us10y_val:.2f}" if isinstance(us10y_val, float) else str(us10y_val)
        
        return (
            f"--- Macro Snapshot ---\n"
            f"{nifty_text}\n"
            f"India VIX: {vix_str}\n"
            f"USD/INR: {usdinr_str}\n"
            f"Brent Crude: {brent_str}\n"
            f"US 10Y Treasury: {us10y_str}%"
        )
    except Exception as e:
        return f"Error fetching macro snapshot: {e}"


def get_breadth() -> str:
    print(f"🔧 [TOOL CALL] get_breadth")
    """
    Computes market breadth metrics from stored signals and prices:
    - % of universe above 50 and 200 DMA
    - New 52-week highs
    - Advance/Decline ratio
    Use to judge if the market environment supports breakouts.
    """
    try:
        with duckdb.connect(DB_PATH, read_only=True) as c:
            # Get latest date
            latest_date = c.execute("SELECT max(date) FROM signals").fetchone()[0]
            if not latest_date:
                return "No signals data available for breadth computation."
                
            # Get previous date for A/D
            prev_date = c.execute("SELECT max(date) FROM signals WHERE date < ?", (latest_date,)).fetchone()[0]
            
            # Query for MAs and Highs
            breadth_df = c.execute("""
                SELECT 
                    count(*) as total,
                    count(CASE WHEN s.above_200ma THEN 1 END) as above_200,
                    count(CASE WHEN p.close > s.sma_50 THEN 1 END) as above_50,
                    count(CASE WHEN s.pct_from_52w_high >= 0 THEN 1 END) as new_highs
                FROM signals s
                JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
                WHERE s.date = ?
            """, (latest_date,)).fetchdf()
            
            # Query for Advance/Decline
            ad_df = c.execute("""
                SELECT 
                    count(CASE WHEN cur.close > prev.close THEN 1 END) as advances,
                    count(CASE WHEN cur.close < prev.close THEN 1 END) as declines
                FROM prices cur
                JOIN prices prev ON cur.symbol = prev.symbol AND prev.date = ?
                WHERE cur.date = ?
            """, (prev_date, latest_date)).fetchdf()
            
            total = breadth_df['total'].iloc[0]
            above_200 = breadth_df['above_200'].iloc[0]
            above_50 = breadth_df['above_50'].iloc[0]
            near_highs = breadth_df['new_highs'].iloc[0]
            
            advances = ad_df['advances'].iloc[0]
            declines = ad_df['declines'].iloc[0]
            
            pct_above_200 = (above_200 / total * 100) if total > 0 else 0
            pct_above_50 = (above_50 / total * 100) if total > 0 else 0
            
            ratio_str = f"{advances/declines:.2f}" if declines > 0 else "N/A"
            
            return (
                f"--- Market Breadth ({latest_date}) ---\n"
                f"Total Universe: {total}\n"
                f"Above 200 DMA: {above_200} ({pct_above_200:.1f}%)\n"
                f"Above 50 DMA: {above_50} ({pct_above_50:.1f}%)\n"
                f"New 52W Highs: {near_highs}\n"
                f"Advance/Decline: {advances}/{declines} (Ratio: {ratio_str})"
            )
    except Exception as e:
        return f"Error computing breadth: {e}"


def get_sector_peers(symbol: str) -> str:
    print(f"🔧 [TOOL CALL] get_sector_peers for {symbol}")
    """
    Fetches key metrics for peers in the same sector as `symbol`.
    Use to compare a candidate with its industry peers.
    """
    try:
        with duckdb.connect(DB_PATH, read_only=True) as c:
            # Get sector for the symbol
            sector_res = c.execute("SELECT sector FROM universe WHERE symbol = ?", (symbol,)).fetchone()
            if not sector_res or not sector_res[0]:
                return f"No sector found for {symbol}."
            sector = sector_res[0]
            
            # Get peers in the same sector
            peers_df = c.execute("""
                SELECT u.symbol, u.company_name, s.rs_rank, s.pct_from_52w_high
                FROM universe u
                LEFT JOIN signals s ON u.symbol = s.symbol
                WHERE u.sector = ? AND u.symbol != ?
                QUALIFY ROW_NUMBER() OVER (PARTITION BY u.symbol ORDER BY s.date DESC) = 1
                ORDER BY s.rs_rank DESC
                LIMIT 10
            """, (sector, symbol)).fetchdf()
            
            return f"--- Peers in Sector: {sector} ---\n{_fmt(peers_df, 'no peers found')}"
    except Exception as e:
        return f"Error fetching sector peers: {e}"


def get_sector_relative_strength(sector: str) -> str:
    print(f"🔧 [TOOL CALL] get_sector_relative_strength for {sector}")
    """
    Computes the average RS rank for all symbols in a given `sector`.
    Use to identify leading sectors.
    """
    try:
        with duckdb.connect(DB_PATH, read_only=True) as c:
            df = c.execute("""
                SELECT AVG(s.rs_rank) as avg_rs_rank, COUNT(DISTINCT u.symbol) as company_count
                FROM universe u
                JOIN signals s ON u.symbol = s.symbol
                WHERE u.sector = ?
                QUALIFY ROW_NUMBER() OVER (PARTITION BY u.symbol ORDER BY s.date DESC) = 1
            """, (sector,)).fetchdf()
            
            if df.empty or pd.isna(df['avg_rs_rank'].iloc[0]):
                return f"No data found for sector {sector}."
                
            avg_rs = df['avg_rs_rank'].iloc[0]
            count = df['company_count'].iloc[0]
            
            return f"Sector: {sector} | Avg RS Rank: {avg_rs:.1f} | Companies: {count}"
    except Exception as e:
        return f"Error computing sector RS: {e}"


def get_earnings_calendar(symbol: str, days_ahead: int = 14) -> str:
    print(f"🔧 [TOOL CALL] get_earnings_calendar for {symbol} (days_ahead={days_ahead})")
    """
    Checks if `symbol` has an earnings date scheduled within the next `days_ahead` days.
    Use to avoid buying right before earnings.
    
    Args:
        symbol: NSE ticker without suffix.
        days_ahead: lookahead window in days (default 14).
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        calendar = ticker.calendar
        
        if not calendar:
            return f"No earnings calendar data available for {symbol}."
            
        earnings_date = calendar.get('Earnings Date')
        if not earnings_date:
            return f"No earnings date scheduled for {symbol}."
            
        if isinstance(earnings_date, list) and len(earnings_date) > 0:
            edate = earnings_date[0]
        elif isinstance(earnings_date, datetime.date):
            edate = earnings_date
        else:
            return f"Unexpected earnings date format for {symbol}."
            
        today = datetime.date.today()
        diff = (edate - today).days
        
        if 0 <= diff <= days_ahead:
            return f"WARNING: {symbol} earnings scheduled in {diff} days on {edate}."
        else:
            return f"{symbol} earnings on {edate} (in {diff} days)."
            
    except Exception as e:
        return f"Error checking earnings calendar for {symbol}: {e}"


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
        days:   number of recent sessions (default 30, capped at 1200).
    """
    print(f"🔧 [TOOL CALL] get_price_history for {symbol} (days={days})")
    days = max(1, min(int(days), 1200))
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
        weeks:  number of recent weeks (default 10, capped at 550).
    """
    print(f"🔧 [TOOL CALL] get_weekly_history for {symbol} (weeks={weeks})")
    weeks = max(1, min(int(weeks), 550))
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute(f"""
            SELECT date, open, high, low, close, volume
            FROM weekly_prices WHERE symbol = ?
            ORDER BY date DESC LIMIT {weeks}
        """, (symbol,)).fetchdf()
    return f"--- {symbol} weekly, last {len(df)} weeks ---\n{_fmt(df, 'no weekly data')}"


def get_fundamentals(symbol: str) -> str:
    print(f"🔧 [TOOL CALL] get_fundamentals for {symbol}")
    """
    Latest annual results (TTM) for `symbol`: EPS, EPS growth YoY, revenue,
    revenue growth YoY, earnings surprise, promoter holding, fetch date.
    Read from `annual_results` table.
    Check whether a technical setup is backed by healthy earnings and stable
    promoter ownership.

    Args:
        symbol: NSE ticker without suffix.
    """
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute("""
            SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy,
                   earnings_surprise, promoter_holding, fetch_date
            FROM annual_results WHERE symbol = ?
            ORDER BY fetch_date DESC LIMIT 1
        """, (symbol,)).fetchdf()
    return _fmt(df, f"no stored annual results for {symbol}")


def get_quarterly_results(symbol: str) -> str:
    print(f"🔧 [TOOL CALL] get_quarterly_results for {symbol}")
    """
    Recent quarterly results for `symbol` to check for earnings acceleration.
    Read from `quarterly_results` table.

    Args:
        symbol: NSE ticker without suffix.
    """
    with duckdb.connect(DB_PATH, read_only=True) as c:
        df = c.execute("""
            SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, net_profit, fetch_date
            FROM quarterly_results WHERE symbol = ?
            ORDER BY quarter DESC LIMIT 6
        """, (symbol,)).fetchdf()
    return _fmt(df, f"no stored quarterly results for {symbol}")


def get_news(symbol: str, days: int = 14) -> str:
    print(f"🔧 [TOOL CALL] get_news for {symbol} (days={days})")
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
    print(f"🔧 [TOOL CALL] get_research_notes for {symbol} (days={days})")
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
    print(f"🔧 [TOOL CALL] get_open_position_detail for {symbol}")
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
    print(f"🔧 [TOOL CALL] get_position_history for {symbol}")
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
    print(f"🔧 [TOOL CALL] execute_read_only_query: {query[:50]}...")
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
