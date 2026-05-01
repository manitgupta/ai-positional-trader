import os
import sys
import duckdb
import pandas as pd
from config import connect_db

# Add project root to path to ensure imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
base_dir = os.path.dirname(src_dir)
sys.path.append(base_dir)

from src.screener.scorer import compute_composite_scores

def run_test_screener(db_path):
    print(f"Running updated screener on DB: {db_path}")
    conn = connect_db(db_path)
    try:
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
            latest_data AS (
                SELECT *,
                       (close - close_3m) / close_3m as ret_3m,
                       (close - close_6m) / close_6m as ret_6m,
                       (close - close_9m) / close_9m as ret_9m,
                       (close - close_12m) / close_12m as ret_12m
                FROM signals_with_prices
                QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY date DESC) = 1
            )
            SELECT *
            FROM latest_data
            WHERE series = 'EQ'
              AND close >= 50
              AND avg_turnover >= 100000000 -- 10 crore
              AND (pct_from_52w_high >= -50 OR sma_50 > sma_200 OR close > close_3m)
        """
        
        candidates = conn.execute(query).fetchdf()
        print(f"Loaded {len(candidates)} stocks passing loose filters.")
        
        if candidates.empty:
            print("No data found or no candidates passed filters.")
            return
            
        # Score candidates
        scored_candidates = compute_composite_scores(candidates)
        # Sort by score
        scored_candidates = scored_candidates.sort_values(by='composite_score', ascending=False)
        
        print("\nTop 10 Candidates:")
        print(scored_candidates[['symbol', 'close', 'rs_rank', 'composite_score']].head(10))
            
    except Exception as e:
        print(f"Error running screener: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    run_test_screener(db_path)
