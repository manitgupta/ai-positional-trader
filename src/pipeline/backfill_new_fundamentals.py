import os
import time
import datetime
import duckdb
from dotenv import load_dotenv
from src.pipeline.fetch_fundamentals import FundamentalsManager

load_dotenv()

# Find DB_PATH relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "universe.duckdb")

def backfill_new_fundamentals():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    # Instantiate manager
    manager = FundamentalsManager(DB_PATH)
    
    # Query for ALL symbols in universe
    conn = duckdb.connect(DB_PATH)
    try:
        symbols_df = conn.execute("""
            SELECT symbol FROM universe 
            WHERE symbol NOT IN (
                SELECT DISTINCT symbol 
                FROM quarterly_results 
                WHERE fetch_date = current_date
            )
        """).fetchdf()
        symbols = symbols_df['symbol'].tolist()
    except Exception as e:
        print(f"Error querying symbols: {e}")
        return
    finally:
        conn.close()
        
    print(f"Found {len(symbols)} symbols in universe to backfill.")
    
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Force updating fundamentals for {symbol}...")
        try:
            # Call update_fundamentals which calls both annual and quarterly
            # and pass force=True to bypass cache
            manager.update_fundamentals(symbol, force=True)
        except Exception as e:
            print(f"Error updating {symbol}: {e}")
            
        time.sleep(3) # Throttling to be safe, increased to 3 seconds
        
    print("Backfill completed!")

if __name__ == "__main__":
    backfill_new_fundamentals()
