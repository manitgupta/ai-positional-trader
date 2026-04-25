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

def backfill_fundamentals():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    # Instantiate manager
    manager = FundamentalsManager(DB_PATH)
    
    # Query for symbols that need update
    # We consider data stale if older than 30 days for backfill purposes
    conn = duckdb.connect(DB_PATH)
    try:
        symbols_df = conn.execute("""
            SELECT u.symbol
            FROM universe u
            WHERE u.symbol NOT IN (
                SELECT DISTINCT symbol 
                FROM quarterly_results
            )
        """).fetchdf()
        symbols = symbols_df['symbol'].tolist()
    except Exception as e:
        print(f"Error querying symbols: {e}")
        return
    finally:
        conn.close()
        
    print(f"Found {len(symbols)} symbols needing fundamentals update.")
    
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Updating fundamentals for {symbol}...")
        try:
            manager.update_quarterly_data(symbol)
        except Exception as e:
            print(f"Error updating {symbol}: {e}")
            
        time.sleep(2) # Throttling to be safe
        
    print("Backfill completed!")

if __name__ == "__main__":
    backfill_fundamentals()
