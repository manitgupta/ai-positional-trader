import os
import datetime
import pandas as pd
import sys
import duckdb
import argparse

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(current_dir)
sys.path.append(base_dir)

from config import DB_PATH, connect_db
from src.pipeline.fetch_prices import PriceFetcher
from src.pipeline.fetch_weekly import WeeklyPriceFetcher
from src.pipeline.compute_signals import SignalComputer
from src.pipeline.fetch_fundamentals import FundamentalsManager
from src.pipeline.fetch_news import NewsFetcher
from src.pipeline.fetch_delivery import DeliveryFetcher
# from src.screener.filters import passes_hard_filters
from src.screener.scorer import compute_composite_scores
from src.analyst.context_builder import ContextBuilder
from src.analyst.gemini_call import GeminiAnalyst
from src.analyst.graph import app as analyst_graph
from src.analyst.parser import extract_json_blocks
from src.portfolio.journal import ResearchJournal
from src.portfolio.manager import PortfolioManager
from src.notifications.telegram import send_research_report



def run_nightly_pipeline(no_journal=False, no_telegram=False):
    print(f"🚀 Starting production nightly pipeline run at {datetime.datetime.now()}")
    
    # Load full universe from DB
    conn = connect_db(DB_PATH)
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
    
    # Fetch daily delivery volume data from NSE
    print("Fetching daily delivery percentage statistics...")
    delivery_fetcher = DeliveryFetcher(DB_PATH)
    delivery_fetcher.fetch_latest()
        
    computer = SignalComputer(DB_PATH)
    print("Checking which symbols need technical signals update...")
    
    conn = connect_db(DB_PATH)
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
    
    if symbols_to_update:
        print("Pre-loading Nifty benchmark prices...")
        nifty_df = computer.load_prices("^NSEI")
        
        # Load enough history for MAs (2 years gives > 200 trading days)
        lookback_date = datetime.date.today() - datetime.timedelta(days=730)
        print(f"Batch loading prices from {lookback_date}...")
        
        all_signals = []
        chunk_size = 100
        
        for i in range(0, len(symbols_to_update), chunk_size):
            chunk = symbols_to_update[i:i+chunk_size]
            print(f"Processing batch {i//chunk_size + 1} ({len(chunk)} symbols)...")
            
            batch_prices = computer.load_prices_batch(chunk, start_date=lookback_date)
            if batch_prices.empty:
                continue
                
            for symbol, df in batch_prices.groupby('symbol'):
                # Extract previous max signal date from our metadata df
                symbol_meta = to_update_df[to_update_df['symbol'] == symbol]
                if symbol_meta.empty:
                    continue
                last_date = symbol_meta.iloc[0]['max_signal_date']
                
                if pd.notna(last_date):
                    if isinstance(last_date, str):
                        last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
                    elif isinstance(last_date, datetime.datetime):
                        last_date = last_date.date()
                        
                # Compute signals
                if not df.empty and len(df) >= 200:
                    signals_df = computer.compute_signals(df, nifty_df=nifty_df)
                    
                    if not signals_df.empty:
                        # Filter to keep only NEW signals beyond last_date
                        if pd.notna(last_date):
                            signals_df['date_obj'] = pd.to_datetime(signals_df['date']).dt.date
                            signals_df = signals_df[signals_df['date_obj'] > last_date]
                            signals_df = signals_df.drop(columns=['date_obj'])
                            
                        if not signals_df.empty:
                            all_signals.append(signals_df)
                            updated_count += 1
                            
        if all_signals:
            combined_signals = pd.concat(all_signals, ignore_index=True)
            print(f"Performing batch save for {len(combined_signals)} calculated signal rows...")
            computer.save_signals(combined_signals)
            
    print(f"Completed technical signals: {updated_count} updated, {skipped_count} skipped.")
            
    # Compute RS Rank percentiles globally
    print("Computing global RS Rank percentiles...")
    conn = connect_db(DB_PATH)
    print("Executing single-query update for RS Rank...")
    try:
        conn.execute("""
            WITH latest_signals AS (
                SELECT symbol, date, raw_momentum_12m,
                       ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY date DESC) as rn
                FROM signals
                WHERE raw_momentum_12m IS NOT NULL
            ),
            ranked_signals AS (
                SELECT symbol, date,
                       CAST(PERCENT_RANK() OVER(ORDER BY raw_momentum_12m) * 100 AS INTEGER) as rs_rank
                FROM latest_signals
                WHERE rn = 1
            )
            UPDATE signals
            SET rs_rank = ranked_signals.rs_rank
            FROM ranked_signals
            WHERE signals.symbol = ranked_signals.symbol AND signals.date = ranked_signals.date;
        """)
        print("Successfully updated RS Rank percentiles.")
    except Exception as e:
        print(f"Error updating RS Rank: {e}")
    finally:
        conn.close()
            
    # 2. Screener - Step 1: Technical Filters & Signals in SQL
    print("\n--- Phase 2: Screener (SQL Filters & Scoring) ---")
    conn = connect_db(DB_PATH)
    
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
    conn = connect_db(DB_PATH)
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
    if os.environ.get("USE_MOTHERDUCK") == "true":
        conn.execute("SET motherduck_dbinstance_inactivity_ttl='0s'")
    conn.close()
    
    # Merge with composite score computed earlier
    scored_candidates = candidate_data.merge(all_candidates[['symbol', 'composite_score']], on='symbol', how='left')
    scored_candidates = scored_candidates.sort_values(by='composite_score', ascending=False)
    
    print("Top candidates passed to Gemini:")
    print(scored_candidates[['symbol', 'composite_score']].head(10))
    
    # Ensure all forced symbols are included in candidates_df passed to graph
    forced_rows = scored_candidates[scored_candidates['symbol'].isin(forced_symbols)]
    non_forced_rows = scored_candidates[~scored_candidates['symbol'].isin(forced_symbols)]
    top_non_forced = non_forced_rows.head(30)
    combined_candidates_df = pd.concat([forced_rows, top_non_forced]).drop_duplicates(subset=['symbol'])
    
    # 5. Gemini Analyst
    print("\n--- Phase 4: Gemini Analyst ---")
    total_candidates = len(candidate_symbols)
    finished_evaluations = 0
    print(f"Running LangGraph flow for {total_candidates} candidates...")
    
    memo = ""
    for chunk in analyst_graph.stream({"candidates": candidate_symbols, "candidates_df": combined_candidates_df}):
        for node_name, state_update in chunk.items():
            if node_name == "evaluate_candidate":
                finished_evaluations += 1
                still_executing = total_candidates - finished_evaluations
                print(f"Progress: {finished_evaluations}/{total_candidates} candidates evaluated. ({still_executing} still executing...)")
            elif node_name == "synthesize_memo":
                memo = state_update.get("final_memo", "")
            else:
                print(f"📍 Node '{node_name}' finished executing.")
    
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
            
            if not no_journal:
                journal.add_entry(symbol, thesis, conviction, action, dec.get('entry_trigger'))
            else:
                print(f"Skipping journal entry for {symbol} (no-journal mode)")

    # 7. Generate Telegram Summary & Send Report
    send_research_report(memo, no_telegram=no_telegram)
    
    print("\n🎉 Pipeline run completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run nightly trading pipeline.")
    parser.add_argument('--no-journal', action='store_true', help="Skip saving entries to research journal.")
    parser.add_argument('--no-telegram', action='store_true', help="Skip generating summary and sending Telegram notifications.")
    args = parser.parse_args()
    
    run_nightly_pipeline(no_journal=args.no_journal, no_telegram=args.no_telegram)
