import yfinance as yf
import duckdb
import os
import sys
import time
from datetime import datetime, timedelta

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import DB_PATH

def backfill_nifty():
    symbol = "^NSEI"
    print(f"Starting backfill for {symbol}...")
    
    # Connect to DB
    conn = duckdb.connect(DB_PATH)
    
    try:
        ticker = yf.Ticker(symbol)
        
        # Fetch daily data
        # Fetch 4 years as requested
        print("Fetching daily data for Nifty 50...")
        df_daily = ticker.history(period="4y")
        
        if not df_daily.empty:
            df_daily = df_daily.reset_index()
            df_daily['symbol'] = symbol
            df_daily['date'] = df_daily['Date'].dt.date
            
            # Insert into prices table
            print(f"Inserting {len(df_daily)} daily rows into prices...")
            conn.register('df_daily_view', df_daily)
            conn.execute("""
                INSERT OR IGNORE INTO prices (symbol, date, open, high, low, close, volume)
                SELECT symbol, date, Open, High, Low, Close, Volume FROM df_daily_view
            """)
        else:
            print("No daily data fetched.")
            
        time.sleep(2) # Rate limiting
        
        # Fetch weekly data
        print("Fetching weekly data for Nifty 50...")
        df_weekly = ticker.history(period="10y", interval="1wk")
        
        if not df_weekly.empty:
            df_weekly = df_weekly.reset_index()
            df_weekly['symbol'] = symbol
            df_weekly['date'] = df_weekly['Date'].dt.date
            
            # Insert into weekly_prices table
            print(f"Inserting {len(df_weekly)} weekly rows into weekly_prices...")
            conn.register('df_weekly_view', df_weekly)
            conn.execute("""
                INSERT OR IGNORE INTO weekly_prices (symbol, date, open, high, low, close, volume)
                SELECT symbol, date, Open, High, Low, Close, Volume FROM df_weekly_view
            """)
        else:
            print("No weekly data fetched.")
            
        print("Nifty 50 backfill completed.")
        
    except Exception as e:
        print(f"Error backfilling Nifty 50: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    backfill_nifty()
