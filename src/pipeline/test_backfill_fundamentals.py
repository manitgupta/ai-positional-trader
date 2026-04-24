import os
import time
import datetime
import pandas as pd
import duckdb
from dotenv import load_dotenv
from src.pipeline.fetch_fundamentals import ScreenerFetcher

load_dotenv()

# Find DB_PATH relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "universe.duckdb")

def test_backfill():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    conn = duckdb.connect(DB_PATH)
    symbols = conn.execute("SELECT symbol FROM universe LIMIT 5").fetchdf()['symbol'].tolist()
    conn.close()
    
    print(f"Starting TEST fundamentals backfill for {len(symbols)} symbols...")
    
    fetcher = ScreenerFetcher()
    
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Fetching fundamentals for {symbol}...")
        try:
            df = fetcher.fetch_fundamentals(symbol)
            if not df.empty:
                print(f"Successfully fetched for {symbol}")
                print(df.to_string(index=False))
            else:
                print(f"No data for {symbol}")
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(1)
        
    print("Test completed!")

if __name__ == "__main__":
    test_backfill()
