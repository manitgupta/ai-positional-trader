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
from src.pipeline.compute_signals import SignalComputer
from src.pipeline.fetch_fundamentals import FundamentalsManager
from src.pipeline.fetch_news import NewsFetcher
from src.screener.filters import passes_hard_filters
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
        
    computer = SignalComputer(DB_PATH)
    print("Computing technical signals for full universe...")
    updated_count = 0
    skipped_count = 0
    
    for symbol in universe_symbols:
        last_date = computer.get_last_signal_date(symbol)
        
        if last_date:
            # We need 252 days of history for 52w high and momentum
            # Let's load from 300 days before last_date to be safe
            start_date = last_date - datetime.timedelta(days=300)
            df = computer.load_prices(symbol, start_date=start_date)
        else:
            df = computer.load_prices(symbol)
            
        if not df.empty and len(df) >= 200:
            signals_df = computer.compute_signals(df)
            
            if last_date:
                # Only keep signals after last_date
                signals_df['date_obj'] = pd.to_datetime(signals_df['date']).dt.date
                signals_df = signals_df[signals_df['date_obj'] > last_date]
                signals_df = signals_df.drop(columns=['date_obj'])
                
            if not signals_df.empty:
                computer.save_signals(signals_df)
                updated_count += 1
            else:
                skipped_count += 1
        else:
            skipped_count += 1
            
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
        latest_signals_df['rs_rank'] = (latest_signals_df['raw_momentum_12m'].rank(pct=True) * 100).astype(int)
        
        for index, row in latest_signals_df.iterrows():
            conn.execute("""
                UPDATE signals 
                SET rs_rank = ? 
                WHERE symbol = ? AND date = ?
            """, (int(row['rs_rank']), row['symbol'], row['date']))
            
    conn.close()
            
    # 2. Screener - Step 1: Technical Hard Filters
    print("\n--- Phase 2: Screener (Technical Hard Filters) ---")
    conn = duckdb.connect(DB_PATH)
    query = """
        SELECT s.*, p.close, p.volume
        FROM signals s
        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
        QUALIFY ROW_NUMBER() OVER(PARTITION BY s.symbol ORDER BY s.date DESC) = 1
    """
    all_data = conn.execute(query).fetchdf()
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
            ) WHERE rn = 1 AND status IN ('watchlist', 'watchlist_entry', 'WATCH_FOR_ENTRY', 'buy_setup')
        """).fetchdf()['symbol'].tolist()
    except Exception:
        pass
        
    forced_symbols = list(set(portfolio_symbols + journal_symbols))
    conn.close()
    
    all_data['passes'] = all_data.apply(passes_hard_filters, axis=1)
    candidates = all_data[all_data['passes'] | all_data['symbol'].isin(forced_symbols)].copy()
    
    print(f"Found {len(candidates)} candidates passing technical filters.")
    
    if candidates.empty:
        print("No candidates passed filters today. Stopping execution.")
        return
        
    candidate_symbols = candidates['symbol'].tolist()
    
    # 3. Data Foundation (Fundamentals & News only for candidates)
    print("\n--- Phase 3: Fundamentals + News for Candidates ---")
    fund_manager = FundamentalsManager(DB_PATH)
    news_fetcher = NewsFetcher(DB_PATH)
    
    for symbol in candidate_symbols:
        fund_manager.update_fundamentals(symbol)
        news_df = news_fetcher.process_news(symbol)
        news_fetcher.save_to_db(news_df)
        
    # 4. Screener - Step 2: Score Candidates
    print("\n--- Phase 2: Screener (Composite Scoring) ---")
    conn = duckdb.connect(DB_PATH)
    query = f"""
        SELECT s.*, p.close, p.volume, f.eps_growth_yoy, f.rev_growth_yoy, f.promoter_holding, f.earnings_surprise
        FROM signals s
        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
        LEFT JOIN fundamentals f ON s.symbol = f.symbol
        WHERE s.symbol IN ({','.join([f"'{s}'" for s in candidate_symbols])})
        QUALIFY ROW_NUMBER() OVER(PARTITION BY s.symbol ORDER BY s.date DESC) = 1
    """
    candidate_data = conn.execute(query).fetchdf()
    conn.close()
    
    scored_candidates = compute_composite_scores(candidate_data)
    scored_candidates = scored_candidates.sort_values(by='composite_score', ascending=False)
    
    print("Top candidates after composite scoring:")
    print(scored_candidates[['symbol', 'composite_score']].head(10))
    
    # 5. Gemini Analyst
    print("\n--- Phase 4: Gemini Analyst ---")
    context_builder = ContextBuilder(DB_PATH)
    context = context_builder.build_context(scored_candidates.head(25))
    
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
            symbol = dec.get('symbol')
            action = dec.get('action')
            thesis = dec.get('thesis')
            conviction = dec.get('conviction')
            
            journal.add_entry(symbol, thesis, conviction, action, dec.get('entry_trigger'))
            
            if action == 'ENTER' or action == 'WATCH_FOR_ENTRY':
                portfolio.open_position(symbol, dec.get('entry_zone', [0])[0], 0, dec.get('stop_loss'), dec.get('target'), dec.get('position_size_pct'), thesis)
            elif action == 'EXIT':
                portfolio.close_position(symbol)
            elif action == 'TRAIL_STOP':
                portfolio.update_stop_loss(symbol, dec.get('new_stop'))

    # 7. Generate Telegram Summary
    print("\n--- Phase 6: Generate Telegram Summary ---")
    summary_prompt = """
    You are a professional equity research editor. Summarize the research memo above into a visually stunning, highly readable Telegram message.
    
    Follow these styling rules to make it look rich and premium:
    1. Use Emojis extensively to add color and structure (e.g., 🚀 for Buy Setups, 👀 for Watchlist, 🎯 for Targets, 🛑 for Stop Loss, 📈 for RS Rank).
    2. Use *ALL CAPS BOLD* for section headers.
    3. Use monospaced code blocks (```) to display key metrics and triggers cleanly.
    4. Keep it under 4000 characters so it fits in a single message.
    5. Do NOT include raw JSON blocks.
    
    Structure the message with:
    - A professional header with the date.
    - A 🚀 *BUY SETUPS* section with clean, structured details for each top candidate.
    - A 👀 *WATCHLIST* section with specific triggers.
    """
    summary = analyst.generate_summary(memo, summary_prompt)
    
    # 8. Notifications
    print("\n--- Phase 7: Notifications ---")
    send_telegram_message(summary)
    
    print("\n🎉 Pipeline run completed.")

if __name__ == "__main__":
    run_nightly_pipeline()
