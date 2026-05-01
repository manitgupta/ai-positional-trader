import os
import time
import datetime
import pandas as pd
import yfinance as yf
import duckdb
from dotenv import load_dotenv
from config import connect_db

load_dotenv()

# Find DB_PATH relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "data", "universe.duckdb")

def backfill_weekly():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return
        
    conn = connect_db(DB_PATH)
    symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    conn.close()
    
    print(f"Starting weekly backfill for {len(symbols)} symbols in batches...")
    
    to_date = datetime.date.today()
    from_date = to_date - datetime.timedelta(days=730) # 2 years
    
    chunk_size = 100
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        tickers = [f"{s}.NS" for s in chunk]
        tickers_str = " ".join(tickers)
        
        print(f"Fetching batch {i // chunk_size + 1} ({len(chunk)} tickers)...")
        try:
            time.sleep(2) # Prevent rate limiting
            df = yf.download(tickers_str, start=from_date, end=to_date, interval="1wk", progress=False, group_by='column')
            
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df = df.stack(level=1).reset_index()
                    df.columns = [col.lower() for col in df.columns]
                    if 'level_1' in df.columns:
                        df = df.rename(columns={'level_1': 'symbol'})
                    elif 'ticker' in df.columns:
                        df = df.rename(columns={'ticker': 'symbol'})
                else:
                    df = df.reset_index()
                    df['symbol'] = chunk[0]
                    df.columns = [col.lower() for col in df.columns]
                    
                df['symbol'] = df['symbol'].astype(str).str.replace('.NS', '', regex=False)
                
                if 'date' not in df.columns and 'datetime' in df.columns:
                    df['date'] = df['datetime']
                    
                df['date'] = pd.to_datetime(df['date']).dt.date
                
                df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
                df = df.dropna(subset=['open', 'high', 'low', 'close'])
                
                conn = connect_db(DB_PATH)
                conn.register('df_view', df)
                conn.execute("""
                    INSERT OR IGNORE INTO weekly_prices 
                    SELECT symbol, date, open, high, low, close, volume 
                    FROM df_view
                """)
                conn.close()
                print(f"Saved {len(df)} weekly rows for batch {i // chunk_size + 1}")
                
        except Exception as e:
            print(f"Error fetching batch starting at {i}: {e}")
            
    print("Backfill completed!")

if __name__ == "__main__":
    backfill_weekly()
