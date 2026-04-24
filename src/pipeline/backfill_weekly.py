import os
import time
import datetime
import pandas as pd
import yfinance as yf
import duckdb
from dotenv import load_dotenv

load_dotenv()

# Find DB_PATH relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "universe.duckdb")

def backfill_weekly():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    conn = duckdb.connect(DB_PATH)
    symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    conn.close()
    
    print(f"Starting weekly backfill for {len(symbols)} symbols...")
    
    to_date = datetime.date.today()
    from_date = to_date - datetime.timedelta(days=730) # 2 years
    
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{len(symbols)}] Fetching weekly data for {symbol}...")
        
        try:
            ticker = symbol + ".NS"
            df = yf.download(ticker, start=from_date, end=to_date, interval="1wk", progress=False)
            
            if not df.empty:
                df = df.reset_index()
                df['symbol'] = symbol
                
                # Standardize column names
                df = df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
                
                # Extract just the date part
                df['date'] = pd.to_datetime(df['date']).dt.date
                
                result_df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
                
                conn = duckdb.connect(DB_PATH)
                conn.register('df_view', result_df)
                conn.execute("""
                    INSERT OR IGNORE INTO weekly_prices 
                    SELECT symbol, date, open, high, low, close, volume 
                    FROM df_view
                """)
                conn.close()
                print(f"Saved {len(result_df)} weekly rows for {symbol}")
            else:
                print(f"No data for {symbol}")
                
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            
        time.sleep(1) # Throttle to avoid rate limits
        
    print("Backfill completed!")

if __name__ == "__main__":
    backfill_weekly()
