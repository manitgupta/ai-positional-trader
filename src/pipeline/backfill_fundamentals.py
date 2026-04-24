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

def backfill_fundamentals():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    conn = duckdb.connect(DB_PATH)
    symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    conn.close()
    
    print(f"Starting fundamentals backfill for {len(symbols)} symbols...")
    
    fetcher = ScreenerFetcher()
    
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Fetching fundamentals for {symbol}...")
        
        try:
            df = fetcher.fetch_fundamentals(symbol)
            
            if not df.empty:
                conn = duckdb.connect(DB_PATH)
                df['fetch_date'] = datetime.date.today()
                
                conn.register('df_view', df)
                conn.execute("""
                    INSERT OR REPLACE INTO fundamentals 
                    SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, 
                           earnings_surprise, roe, debt_to_equity, promoter_holding, fetch_date
                    FROM df_view
                """)
                conn.close()
                print(f"Saved fundamentals for {symbol}")
            else:
                print(f"No data for {symbol}")
                
        except Exception as e:
            print(f"Error fetching fundamentals for {symbol}: {e}")
            
        time.sleep(1) # Throttle to avoid rate limits
        
    print("Backfill completed!")

if __name__ == "__main__":
    backfill_fundamentals()
