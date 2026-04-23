import os
import sys
import duckdb
import pandas as pd

# Add project root to path to ensure imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
base_dir = os.path.dirname(src_dir)
sys.path.append(base_dir)

from src.screener.filters import passes_hard_filters
from src.screener.scorer import compute_composite_scores

def run_test_screener(db_path):
    print(f"Running screener on DB: {db_path}")
    conn = duckdb.connect(db_path)
    try:
        # Get latest signals for all stocks
        # We join with prices to get the latest close price as well
        query = """
            SELECT s.*, p.close, p.volume 
            FROM signals s
            JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
            QUALIFY ROW_NUMBER() OVER(PARTITION BY s.symbol ORDER BY s.date DESC) = 1
        """
        df = conn.execute(query).fetchdf()
        print(f"Loaded {len(df)} stocks with latest signals.")
        
        if df.empty:
            print("No data found in signals table.")
            return
            
        # Apply filters
        df['passes'] = df.apply(passes_hard_filters, axis=1)
        candidates = df[df['passes']].copy()
        
        print(f"Found {len(candidates)} candidates passing filters.")
        
        if not candidates.empty:
            # Score candidates
            scored_candidates = compute_composite_scores(candidates)
            # Sort by score
            scored_candidates = scored_candidates.sort_values(by='composite_score', ascending=False)
            print("\nTop Candidates:")
            print(scored_candidates[['symbol', 'close', 'rs_rank', 'composite_score']])
        else:
            print("No candidates passed the hard filters.")
            
            # Print why they failed (show data for one)
            print("\nSample data for a stock (RELIANCE):")
            rel_data = df[df['symbol'] == 'RELIANCE']
            if not rel_data.empty:
                print(rel_data.iloc[0].to_dict())
            else:
                print("No data for RELIANCE found.")
            
    except Exception as e:
        print(f"Error running screener: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    run_test_screener(db_path)
