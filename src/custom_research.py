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
from src.pipeline.fetch_fundamentals import FundamentalsManager
from src.pipeline.fetch_news import NewsFetcher
from src.analyst.graph import app as analyst_graph
from src.analyst.parser import extract_json_blocks

def run_custom_research(candidate_symbols):
    print(f"🚀 Starting custom research for: {candidate_symbols} at {datetime.datetime.now()}")
    
    if not candidate_symbols:
        print("No symbols provided.")
        return

    # 1. Data Foundation (Fundamentals & News for specified candidates)
    print("\n--- Phase 1: Fundamentals + News for Candidates ---")
    fund_manager = FundamentalsManager(DB_PATH)
    news_fetcher = NewsFetcher(DB_PATH)
    
    for symbol in candidate_symbols:
        fund_manager.update_fundamentals(symbol)
        news_df = news_fetcher.process_news(symbol)
        news_fetcher.save_to_db(news_df)
        
    # Refresh data with fundamentals for Gemini
    conn = duckdb.connect(DB_PATH)
    
    # Construct query to get candidate data
    # We assume prices and signals already exist in the DB for these symbols
    symbols_str = ','.join([f"'{s}'" for s in candidate_symbols])
    
    query = f"""
        WITH quarterly_growth AS (
            SELECT symbol, quarter, eps_growth_yoy, rev_growth_yoy,
                   LAG(eps_growth_yoy, 1) OVER(PARTITION BY symbol ORDER BY quarter) as prev_eps_growth,
                   LAG(eps_growth_yoy, 2) OVER(PARTITION BY symbol ORDER BY quarter) as prev2_eps_growth,
                   LAG(rev_growth_yoy, 1) OVER(PARTITION BY symbol ORDER BY quarter) as prev_rev_growth,
                   LAG(rev_growth_yoy, 2) OVER(PARTITION BY symbol ORDER BY quarter) as prev2_rev_growth
            FROM quarterly_results
            WHERE symbol IN ({symbols_str})
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
            WHERE symbol IN ({symbols_str})
            QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY fetch_date DESC) = 1
        )
        SELECT s.*, p.close, p.volume, 
               a.eps_growth_yoy as annual_eps_growth, a.rev_growth_yoy as annual_rev_growth,
               q.code33_eps, q.code33_rev, a.promoter_holding, a.earnings_surprise
        FROM signals s
        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
        LEFT JOIN code33_status q ON s.symbol = q.symbol
        LEFT JOIN latest_annual a ON s.symbol = a.symbol
        WHERE s.symbol IN ({symbols_str})
        QUALIFY ROW_NUMBER() OVER(PARTITION BY s.symbol ORDER BY s.date DESC) = 1
    """
    
    try:
        candidate_data = conn.execute(query).fetchdf()
    except Exception as e:
        print(f"Error querying database: {e}")
        conn.close()
        return
        
    conn.close()
    
    if candidate_data.empty:
        print("No data found for the specified candidates in signals/prices table.")
        print("Please ensure they are in the universe and have prices/signals calculated.")
        # We still proceed with just the symbols if data is missing, 
        # as the graph builder might be able to handle it or Gemini might fetch it.
        # But we need a DataFrame with at least the symbol column.
        candidate_data = pd.DataFrame({'symbol': candidate_symbols})
    
    print(f"Found data for {len(candidate_data)} candidates.")
    print(candidate_data[['symbol']].head())
    
    # 2. Gemini Analyst
    print("\n--- Phase 2: Gemini Analyst ---")
    print(f"Running LangGraph flow for {len(candidate_symbols)} candidates...")
    
    try:
        graph_result = analyst_graph.invoke({"candidates": candidate_symbols, "candidates_df": candidate_data})
        memo = graph_result.get("final_memo", "")
        
        print("\n--- Memo Generated ---")
        print(memo)
        
    except Exception as e:
        print(f"Error running LangGraph flow: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/custom_research.py SYMBOL1 SYMBOL2 ...")
        print("Example: python src/custom_research.py RELIANCE TCS")
        sys.exit(1)
        
    symbols = sys.argv[1:]
    run_custom_research(symbols)
