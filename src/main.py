import os
import datetime
import pandas as pd
import sys
import duckdb

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(current_dir)
sys.path.append(base_dir)

from config import DB_PATH
from src.pipeline.fetch_prices import PriceFetcher
from src.pipeline.fetch_weekly import WeeklyPriceFetcher
from src.pipeline.compute_signals import SignalComputer
from src.pipeline.fetch_fundamentals import FundamentalsManager
from src.pipeline.fetch_news import NewsFetcher
# from src.screener.filters import passes_hard_filters
from src.screener.scorer import compute_composite_scores
from src.analyst.context_builder import ContextBuilder
from src.analyst.gemini_call import GeminiAnalyst
from src.analyst.parser import extract_json_blocks
from src.portfolio.journal import ResearchJournal
from src.portfolio.manager import PortfolioManager
from src.notifications.telegram import send_telegram_message

def run_nightly_pipeline():
    print(f"🚀 Starting production nightly pipeline run at {datetime.datetime.now()}")
    
    # Load full universe from DB
    conn = duckdb.connect(DB_PATH)
    universe_symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    conn.close()
    
    print(f"Loaded {len(universe_symbols)} symbols from universe.")
    
    # 1. Data Foundation (Prices & Signals for full universe)
    print("\n--- Phase 1: Data Foundation (Prices & Technical Signals) ---")
    fetcher = PriceFetcher(DB_PATH)
    to_date = datetime.date.today()
    from_date = to_date - datetime.timedelta(days=365) # 1 year for MAs
    
    # Fetch prices in batches
    price_df = fetcher.fetch_batch_eod_data(universe_symbols, from_date, to_date, chunk_size=100)
    fetcher.save_to_db(price_df)
    
    # Fetch weekly prices in batches
    print("Fetching weekly prices for full universe...")
    weekly_fetcher = WeeklyPriceFetcher(DB_PATH)
    weekly_df = weekly_fetcher.fetch_batch_weekly_data(universe_symbols, chunk_size=100)
    weekly_fetcher.save_to_db(weekly_df)
        
    computer = SignalComputer(DB_PATH)
    print("Checking which symbols need technical signals update...")
    
    conn = duckdb.connect(DB_PATH)
    to_update_df = conn.execute("""
        SELECT p.symbol, MAX(s.date) as max_signal_date
        FROM prices p
        LEFT JOIN signals s ON p.symbol = s.symbol
        GROUP BY p.symbol
        HAVING MAX(p.date) > MAX(s.date) OR MAX(s.date) IS NULL
    """).fetchdf()
    conn.close()
    
    symbols_to_update = to_update_df['symbol'].tolist()
    print(f"{len(symbols_to_update)} symbols need signal computation.")
    
    updated_count = 0
    skipped_count = len(universe_symbols) - len(symbols_to_update)
    
    for symbol in symbols_to_update:
        row = to_update_df[to_update_df['symbol'] == symbol].iloc[0]
        last_date = row['max_signal_date']
        
        if pd.notna(last_date):
            if isinstance(last_date, str):
                last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
            elif isinstance(last_date, datetime.datetime):
                last_date = last_date.date()
            elif isinstance(last_date, datetime.date):
                last_date = last_date
                
            start_date = last_date - datetime.timedelta(days=300)
            df = computer.load_prices(symbol, start_date=start_date)
        else:
            df = computer.load_prices(symbol)
            last_date = None
            
        if not df.empty and len(df) >= 200:
            signals_df = computer.compute_signals(df)
            
            if last_date:
                signals_df['date_obj'] = pd.to_datetime(signals_df['date']).dt.date
                signals_df = signals_df[signals_df['date_obj'] > last_date]
                signals_df = signals_df.drop(columns=['date_obj'])
                
            if not signals_df.empty:
                computer.save_signals(signals_df)
                updated_count += 1
            
    print(f"Completed technical signals: {updated_count} updated, {skipped_count} skipped.")
            
    # Compute RS Rank percentiles globally
    print("Computing global RS Rank percentiles...")
    conn = duckdb.connect(DB_PATH)
    latest_signals_df = conn.execute("""
        SELECT symbol, date, raw_momentum_12m
        FROM signals
        QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY date DESC) = 1
    """).fetchdf()
    
    if not latest_signals_df.empty:
        latest_signals_df = latest_signals_df.dropna(subset=['raw_momentum_12m'])
        if not latest_signals_df.empty:
            latest_signals_df['rs_rank'] = (latest_signals_df['raw_momentum_12m'].rank(pct=True) * 100).astype(int)
        
        for index, row in latest_signals_df.iterrows():
            conn.execute("""
                UPDATE signals 
                SET rs_rank = ? 
                WHERE symbol = ? AND date = ?
            """, (int(row['rs_rank']), row['symbol'], row['date']))
            
    conn.close()
            
    # 2. Screener - Step 1: Technical Filters & Signals in SQL
    print("\n--- Phase 2: Screener (SQL Filters & Scoring) ---")
    conn = duckdb.connect(DB_PATH)
    
    # Query to compute returns and turnover in SQL, and apply loose filters
    query = """
        WITH signals_with_prices AS (
            SELECT s.*, p.close, p.volume, u.series, u.sector,
                   LAG(p.close, 63) OVER(PARTITION BY s.symbol ORDER BY s.date) as close_3m,
                   LAG(p.close, 126) OVER(PARTITION BY s.symbol ORDER BY s.date) as close_6m,
                   LAG(p.close, 189) OVER(PARTITION BY s.symbol ORDER BY s.date) as close_9m,
                   LAG(p.close, 252) OVER(PARTITION BY s.symbol ORDER BY s.date) as close_12m,
                   AVG(p.volume * p.close) OVER(PARTITION BY s.symbol ORDER BY s.date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as avg_turnover
            FROM signals s
            JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
            JOIN universe u ON s.symbol = u.symbol
        ),
        quarterly_growth AS (
            SELECT symbol, quarter, eps_growth_yoy, rev_growth_yoy,
                   LAG(eps_growth_yoy, 1) OVER(PARTITION BY symbol ORDER BY quarter) as prev_eps_growth,
                   LAG(eps_growth_yoy, 2) OVER(PARTITION BY symbol ORDER BY quarter) as prev2_eps_growth,
                   LAG(rev_growth_yoy, 1) OVER(PARTITION BY symbol ORDER BY quarter) as prev_rev_growth,
                   LAG(rev_growth_yoy, 2) OVER(PARTITION BY symbol ORDER BY quarter) as prev2_rev_growth
            FROM quarterly_results
        ),
        code33_status AS (
            SELECT symbol,
                   (eps_growth_yoy > prev_eps_growth AND prev_eps_growth > prev2_eps_growth) as code33_eps,
                   (rev_growth_yoy > prev_rev_growth AND prev_rev_growth > prev2_rev_growth) as code33_rev
            FROM quarterly_growth
            QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY quarter DESC) = 1
        ),
        latest_data AS (
            SELECT s.*,
                   (s.close - s.close_3m) / s.close_3m as ret_3m,
                   (s.close - s.close_6m) / s.close_6m as ret_6m,
                   (s.close - s.close_9m) / s.close_9m as ret_9m,
                   (s.close - s.close_12m) / s.close_12m as ret_12m,
                   q.code33_eps, q.code33_rev
            FROM signals_with_prices s
            LEFT JOIN code33_status q ON s.symbol = q.symbol
            QUALIFY ROW_NUMBER() OVER(PARTITION BY s.symbol ORDER BY s.date DESC) = 1
        )
        SELECT *
        FROM latest_data
        WHERE series = 'EQ'
          AND close >= 50
          AND avg_turnover >= 100000000 -- 10 crore
          AND (pct_from_52w_high >= -50 OR sma_50 > sma_200 OR close > close_3m)
    """
    
    all_candidates = conn.execute(query).fetchdf()
    
    # Get forced symbols (portfolio + watchlist)
    portfolio_symbols = []
    try:
        portfolio_symbols = conn.execute("SELECT symbol FROM portfolio WHERE status = 'OPEN'").fetchdf()['symbol'].tolist()
    except Exception:
        pass
        
    journal_symbols = []
    try:
        journal_symbols = conn.execute("""
            SELECT symbol FROM (
                SELECT symbol, status, date,
                ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY date DESC) as rn
                FROM research_journal
                WHERE date > current_date - INTERVAL 30 DAY
            ) WHERE rn = 1 AND status IN ('watchlist', 'watchlist_entry', 'WATCH_FOR_ENTRY', 'buy_setup')
        """).fetchdf()['symbol'].tolist()
    except Exception:
        pass
        
    forced_symbols = list(set(portfolio_symbols + journal_symbols))
    
    # Combine query results with forced symbols
    conn.close()
    
    # If forced symbols are missing from all_candidates, we should ideally fetch them.
    # For now, we just ensure they are included if they were in the signals table.
    # (Simulated by adding them to the dataframe if missing and available in DB)
    
    print(f"Found {len(all_candidates)} candidates passing loose filters.")
    
    # Compute composite score
    if not all_candidates.empty:
        all_candidates = compute_composite_scores(all_candidates)
        all_candidates = all_candidates.sort_values(by='composite_score', ascending=False)
        
    # Take top 30 candidates
    top_candidates = all_candidates.head(30)
    candidate_symbols = top_candidates['symbol'].tolist()
    
    # Add forced symbols if not in top 30
    for sym in forced_symbols:
        if sym not in candidate_symbols:
            candidate_symbols.append(sym)
            
    print(f"Final candidate list size: {len(candidate_symbols)}")
    
    # 3. Data Foundation (Fundamentals & News only for top candidates)
    print("\n--- Phase 3: Fundamentals + News for Candidates ---")
    fund_manager = FundamentalsManager(DB_PATH)
    news_fetcher = NewsFetcher(DB_PATH)
    
    for symbol in candidate_symbols:
        fund_manager.update_fundamentals(symbol)
        news_df = news_fetcher.process_news(symbol)
        news_fetcher.save_to_db(news_df)
        
    # Refresh data with fundamentals for Gemini
    conn = duckdb.connect(DB_PATH)
    query = f"""
        WITH quarterly_growth AS (
            SELECT symbol, quarter, eps_growth_yoy, rev_growth_yoy,
                   LAG(eps_growth_yoy, 1) OVER(PARTITION BY symbol ORDER BY quarter) as prev_eps_growth,
                   LAG(eps_growth_yoy, 2) OVER(PARTITION BY symbol ORDER BY quarter) as prev2_eps_growth,
                   LAG(rev_growth_yoy, 1) OVER(PARTITION BY symbol ORDER BY quarter) as prev_rev_growth,
                   LAG(rev_growth_yoy, 2) OVER(PARTITION BY symbol ORDER BY quarter) as prev2_rev_growth
            FROM quarterly_results
            WHERE symbol IN ({','.join([f"'{s}'" for s in candidate_symbols])})
        ),
        code33_status AS (
            SELECT symbol, quarter,
                   (eps_growth_yoy > prev_eps_growth AND prev_eps_growth > prev2_eps_growth) as code33_eps,
                   (rev_growth_yoy > prev_rev_growth AND prev_rev_growth > prev2_rev_growth) as code33_rev
            FROM quarterly_growth
            QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY quarter DESC) = 1
        ),
        latest_annual AS (
            SELECT symbol, eps_growth_yoy, rev_growth_yoy, promoter_holding, earnings_surprise
            FROM annual_results
            WHERE symbol IN ({','.join([f"'{s}'" for s in candidate_symbols])})
            QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY fetch_date DESC) = 1
        )
        SELECT s.*, p.close, p.volume, 
               a.eps_growth_yoy as annual_eps_growth, a.rev_growth_yoy as annual_rev_growth,
               q.code33_eps, q.code33_rev, a.promoter_holding, a.earnings_surprise
        FROM signals s
        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
        LEFT JOIN code33_status q ON s.symbol = q.symbol
        LEFT JOIN latest_annual a ON s.symbol = a.symbol
        WHERE s.symbol IN ({','.join([f"'{s}'" for s in candidate_symbols])})
        QUALIFY ROW_NUMBER() OVER(PARTITION BY s.symbol ORDER BY s.date DESC) = 1
    """
    candidate_data = conn.execute(query).fetchdf()
    conn.close()
    
    # Merge with composite score computed earlier
    scored_candidates = candidate_data.merge(all_candidates[['symbol', 'composite_score']], on='symbol', how='left')
    scored_candidates = scored_candidates.sort_values(by='composite_score', ascending=False)
    
    print("Top candidates passed to Gemini:")
    print(scored_candidates[['symbol', 'composite_score']].head(10))
    
    # 5. Gemini Analyst
    print("\n--- Phase 4: Gemini Analyst ---")
    context_builder = ContextBuilder(DB_PATH)
    context = context_builder.build_context(scored_candidates.head(30))
    
    analyst = GeminiAnalyst()
    memo = analyst.generate_memo(context)
    
    print("\n--- Memo Generated ---")
    print(memo)
    
    # 6. Parser + Journal + Portfolio
    print("\n--- Phase 5: Journal + Memory ---")
    decisions = extract_json_blocks(memo)
    print(f"Extracted {len(decisions)} decision blocks.")
    
    journal = ResearchJournal(DB_PATH)
    portfolio = PortfolioManager(DB_PATH)
    
    for block in decisions:
        section = block.get('section')
        decisions_list = block.get('decisions', [])
        
        for dec in decisions_list:
            symbol = dec.get('symbol') or dec.get('ticker')
            action = dec.get('action')
            thesis = dec.get('thesis')
            conviction = dec.get('conviction')
            
            journal.add_entry(symbol, thesis, conviction, action, dec.get('entry_trigger'))
            
            # Positions are opened manually by the user. Auto-open disabled.
            # if action == 'ENTER' or action == 'WATCH_FOR_ENTRY':
            #     portfolio.open_position(symbol, dec.get('entry_zone', [0])[0], 0, dec.get('stop_loss'), dec.get('target'), dec.get('position_size_pct'), thesis)
            if action == 'EXIT':
                portfolio.close_position(symbol)
            elif action == 'TRAIL_STOP':
                portfolio.update_stop_loss(symbol, dec.get('new_stop'))

    # 7. Generate Telegram Summary
    print("\n--- Phase 6: Generate Telegram Summary ---")
    today = datetime.date.today().strftime("%B %d, %Y")
    summary_prompt = f"""
    You are a professional equity research editor. Summarize the research memo above into a visually stunning, highly readable Telegram message using HTML tags.
    
    Current Date: {today}
    
    Follow these styling rules to make it look rich and premium:
    1. Use Emojis extensively to add color and structure (e.g., 🚀 for Buy Setups, 👀 for Watchlist, 🎯 for Targets, 🛑 for Stop Loss, 📈 for RS Rank).
    2. Use <b>ALL CAPS BOLD</b> for section headers.
    3. Use <pre>...</pre> to display key metrics and triggers cleanly.
    4. Keep it under 4000 characters so it fits in a single message.
    5. Output ONLY valid HTML. Do NOT use Markdown tags like ** or *.
    
    Telegram supports only these tags: <b>, <i>, <u>, <s>, <a>, <code>, <pre>. Do NOT use any other tags like <p>, <h1>, <ul> etc.
    
    Structure the message with:
    - A professional header with the date {today}.
    - A 📊 <b>PORTFOLIO REVIEW</b> section summarizing the status of open positions and any actions needed.
    - A 🚀 <b>BUY SETUPS</b> section with clean, structured details for each top candidate.
    - A 👀 <b>WATCHLIST</b> section with specific triggers.
    """
    summary = analyst.generate_summary(memo, summary_prompt)
    
    # 8. Notifications
    print("\n--- Phase 7: Notifications ---")
    send_telegram_message(summary)
    
    print("\n🎉 Pipeline run completed.")

if __name__ == "__main__":
    run_nightly_pipeline()
