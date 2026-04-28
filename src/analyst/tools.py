"""Tool functions registered with Gemini. All return formatted strings."""
import datetime
import duckdb
import pandas as pd
import yfinance as yf
from config import DB_PATH

def get_macro_snapshot() -> str:
    print(f"🔧 [TOOL CALL] get_macro_snapshot")
    """
    Fetches a snapshot of key macroeconomic indicators to assess market environment risk.
    
    Returns a formatted string with:
    - Nifty 50 (^NSEI) level, daily % change, and percentage distance from its 50 and 200 Day Moving Averages.
    - India VIX (^INDIAVIX) level (market volatility expectation).
    - USD/INR exchange rate (currency risk/strength).
    - Brent Crude price in USD (energy cost impact on Indian economy).
    - US 10-Year Treasury Yield (%) (global interest rate benchmark).
    
    Use this tool at the beginning of your analysis to understand if the macro environment is supportive (bullish), risky (volatile), or hostile for equity breakouts.
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
    Computes market breadth metrics from stored signals and prices for the latest available date.
    
    Returns a formatted string with:
    - Total number of stocks in the universe.
    - Count and percentage of stocks trading above their 200-day moving average (long-term breadth).
    - Count and percentage of stocks trading above their 50-day moving average (medium-term breadth).
    - Count of stocks making new 52-week highs.
    - Advance/Decline ratio for the latest session (advancing stocks / declining stocks).
    
    High percentages (>60-70%) above moving averages indicate a healthy, trending market supportive of breakouts. Low percentages (<30%) indicate a weak market where breakouts are likely to fail.
    """
    try:
        with duckdb.connect(DB_PATH) as c:
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
    Fetches key metrics for peers in the same sector as the requested `symbol`.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        
    Returns a table of up to 10 peers in the same sector, ordered by Relative Strength (RS) rank descending.
    Columns include: Symbol, Company Name, RS Rank, and % from 52-week high.
    
    Use this tool to determine if the candidate stock is a leader in its sector (highest RS rank) or a laggard, and to see if the sector as a whole is showing strength.
    """
    try:
        with duckdb.connect(DB_PATH) as c:
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
    Computes the average Relative Strength (RS) rank for all symbols in a given `sector`.
    
    Args:
        sector: The name of the sector (e.g., "Information Technology", "Financial Services").
        
    Returns a string with the sector name, average RS rank, and the number of companies in that sector.
    
    Use this tool to identify leading sectors. Positional traders prefer to buy the best stocks in the best (highest average RS) sectors.
    """
    try:
        with duckdb.connect(DB_PATH) as c:
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
    Checks if the requested `symbol` has an earnings announcement scheduled within the next `days_ahead` days.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        days_ahead: Lookahead window in days (default 14).
        
    Returns a warning message if earnings are scheduled within the window, or a status message with the date.
    
    CRITICAL USE: Always check this before recommending an entry. Position sizing should be reduced or entries avoided immediately before earnings to prevent gap-down risk.
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
    Fetches daily price history and technical indicators for a requested `symbol`.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        days: Number of recent trading sessions to return (default 30, capped at 1200).
        
    Returns a table with the following columns:
    - `date`: Trading date.
    - `close`: Daily closing price.
    - `volume`: Daily volume.
    - `rsi_14`: Relative Strength Index (14-day). Momentum oscillator.
    - `adx_14`: Average Directional Index (14-day). Measures trend strength.
    - `atr_14`: Average True Range (14-day). Measures volatility.
    - `macd_hist`: MACD Histogram. Identifies momentum shifts.
    - `sma_50`, `sma_150`, `sma_200`: Simple Moving Averages. Key trend indicators.
    - `rs_rank`: Percentile rank (0-100) vs the universe based on 12m return.
    - `pct_from_52w_high`: How close the stock is to its yearly high.
    - `volume_ratio_20d`: Current volume vs 20-day average.
    - `bb_width`: Bollinger Band Width. Low values indicate a squeeze.
    - `daily_rs`: 20-day smoothed ratio of stock to Nifty 50.
    
    Use this to analyze the daily setup, identify bases, breakouts, and stack of moving averages.
    """
    print(f"🔧 [TOOL CALL] get_price_history for {symbol} (days={days})")
    days = max(1, min(int(days), 1200))
    with duckdb.connect(DB_PATH) as c:
        df = c.execute(f"""
            SELECT s.date, p.close, p.volume,
                   s.rsi_14, s.adx_14, s.atr_14, s.macd_hist,
                   s.sma_50, s.sma_150, s.sma_200,
                   s.rs_rank, s.pct_from_52w_high, s.volume_ratio_20d,
                   s.bb_width, s.daily_rs
            FROM signals s
            JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
            WHERE s.symbol = ?
            ORDER BY s.date DESC
            LIMIT {days}
        """, (symbol,)).fetchdf()
    return f"--- {symbol} daily, last {len(df)} sessions ---\n{_fmt(df, 'no data')}"


def get_weekly_history(symbol: str, weeks: int = 10) -> str:
    """
    Fetches weekly price history and technical signals for a requested `symbol`.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        weeks: Number of recent weeks to return (default 10, capped at 550).
        
    Returns a table with the following columns:
    - `date`: End date of the week.
    - `open`, `high`, `low`, `close`, `volume`: Weekly OHLCV data.
    - `sma_10`: 10-week Moving Average (short-term trend guide).
    - `sma_30`: 30-week Moving Average (core Stage-2 trend guide).
    - `rsi_14`: Weekly RSI for long-term momentum.
    - `volume_ratio_10w`: Weekly volume vs 10-week average.
    - `mansfield_rs`: Mansfield Relative Strength vs Nifty 50. Positive values indicate outperformance.
    
    Use this to confirm the weekly Stage-2 context: price above rising 30-week MA, expanding volume on up weeks, and positive Mansfield RS.
    """
    print(f"🔧 [TOOL CALL] get_weekly_history for {symbol} (weeks={weeks})")
    weeks = max(1, min(int(weeks), 550))
    with duckdb.connect(DB_PATH) as c:
        df = c.execute(f"""
            SELECT p.date, p.open, p.high, p.low, p.close, p.volume,
                   s.sma_10, s.sma_30, s.rsi_14, s.volume_ratio_10w, s.mansfield_rs
            FROM weekly_prices p
            LEFT JOIN weekly_signals s ON p.symbol = s.symbol AND p.date = s.date
            WHERE p.symbol = ?
            ORDER BY p.date DESC LIMIT {weeks}
        """, (symbol,)).fetchdf()
    return f"--- {symbol} weekly, last {len(df)} weeks ---\n{_fmt(df, 'no weekly data')}"


def get_annual_fundamentals(symbol: str) -> str:
    print(f"🔧 [TOOL CALL] get_annual_fundamentals for {symbol}")
    """
    Fetches the latest annual fundamental results and historical trend for the requested `symbol`.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        
    Returns a table with:
    - `quarter`: The reported year or TTM (e.g., "Mar 2024", "TTM").
    - `eps`: Earnings Per Share.
    - `eps_growth_yoy`: Year-over-Year EPS growth percentage.
    - `revenue`: Total revenue.
    - `rev_growth_yoy`: Year-over-Year revenue growth percentage.
    - `earnings_surprise`: Percentage beat or miss vs consensus estimates.
    - `roe`: Return on Equity (latest available).
    - `debt_to_equity`: Debt to Equity ratio (latest available).
    - `promoter_holding`: Percentage of shares held by promoters.
    
    Use this tool to verify that a technical setup is backed by strong fundamental growth, good ratios, and high/stable promoter ownership.
    """
    with duckdb.connect(DB_PATH) as c:
        df = c.execute("""
            WITH latest_fetch AS (
                SELECT MAX(fetch_date) as max_date FROM annual_results WHERE symbol = ?
            )
            SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy,
                   earnings_surprise, roe, debt_to_equity, promoter_holding, fetch_date
            FROM annual_results 
            WHERE symbol = ? AND fetch_date = (SELECT max_date FROM latest_fetch)
            ORDER BY quarter DESC
        """, (symbol, symbol)).fetchdf()
    return _fmt(df, f"no stored annual results for {symbol}")


def get_quarterly_fundamentals(symbol: str) -> str:
    print(f"🔧 [TOOL CALL] get_quarterly_fundamentals for {symbol}")
    """
    Fetches the last 6 quarters of results for the requested `symbol`, including shareholding trend.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        
    Returns a table showing quarterly progression of EPS, EPS Growth YoY, Revenue, Revenue Growth YoY, Net Profit, and Shareholding (Promoter, FII, DII).
    
    Use this tool to check for earnings acceleration and increasing institutional/promoter interest over recent quarters.
    """
    with duckdb.connect(DB_PATH) as c:
        df = c.execute("""
            SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, net_profit,
                   promoter_holding, fii_holding, dii_holding, fetch_date
            FROM quarterly_results WHERE symbol = ?
            ORDER BY quarter DESC LIMIT 6
        """, (symbol,)).fetchdf()
    return _fmt(df, f"no stored quarterly results for {symbol}")


def get_news(symbol: str, days: int = 14) -> str:
    print(f"🔧 [TOOL CALL] get_news for {symbol} (days={days})")
    """
    Fetches stored news sentiment and summaries for the requested `symbol`.
    
    Args:
        symbol: NSE ticker without suffix (e.g., "RELIANCE").
        days: Lookback window in days (default 14, capped at 90).
        
    Returns a table with:
    - `date`: Date of the news.
    - `sentiment_score`: Score from -1 (negative) to +1 (positive).
    - `material_event`: Boolean flag indicating a major corporate action or event.
    - `summary`: A brief summary of the news item.
    
    Use this tool to check for recent news that might explain price action. For fresher news or to dig deeper into material events, use the `search_web` tool as a fallback.
    """
    days = max(1, min(int(days), 90))
    with duckdb.connect(DB_PATH) as c:
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
    Fetches your own prior research notes from the research journal.
    
    Args:
        symbol: NSE ticker without suffix, or "" to get a summary of all recent notes.
        days: Lookback window in days (default 45, capped at 365).
        
    Returns:
    - If `symbol` is provided: Full note details including thesis, conviction, status, entry trigger, and risk factors.
    - If `symbol` is "": A summary table of all symbols with notes in the window, showing symbol, date, conviction, status, and entry trigger.
    
    Use this tool to maintain continuity across your research sessions and recall why you were watching or rejected a stock.
    """
    days = max(1, min(int(days), 365))
    with duckdb.connect(DB_PATH) as c:
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
    Fetches detailed current context for open positions in your portfolio.
    
    Args:
        symbol: NSE ticker without suffix, or "" for all open positions.
        
    Returns a table with:
    - `symbol`: Stock symbol.
    - `entry_date`, `entry_price`, `quantity`: Original trade details.
    - `stop_loss`, `target`: Planned risk/reward levels.
    - `position_pct`: Size of position as % of portfolio.
    - `thesis_summary`: Brief rationale for the trade.
    - `current_close`: Latest closing price.
    - `pnl_pct`: Current Profit/Loss percentage.
    - `rsi_14`, `adx_14`: Latest daily momentum indicators.
    
    Use this tool to review your portfolio, check if trailing stops are needed, or if a thesis is broken.
    """
    where = "WHERE p.status = 'OPEN'"
    args: tuple = ()
    if symbol:
        where += " AND p.symbol = ?"
        args = (symbol,)
    with duckdb.connect(DB_PATH) as c:
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
    Fetches price and signal history for an open position in two critical windows.
    
    Args:
        symbol: NSE ticker without suffix. Must be a symbol with an OPEN status in your portfolio.
        
    Returns two tables separated by headers:
    1. **Around Entry**: Daily data for ±15 trading days around the entry date (helps recall the setup you bought).
    2. **Latest**: Daily data for the last 15 sessions (shows current state).
    Columns include: date, close, volume, rsi_14, adx_14, sma_50, sma_200.
    
    Use this tool to judge if a position's thesis is still intact or if the character of the stock has changed negatively since entry.
    """
    with duckdb.connect(DB_PATH) as c:
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


