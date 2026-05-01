import duckdb
import yfinance as yf
import time
import datetime
import os
import sys

# Add project root to path to import config
sys.path.append('/Users/manitgupta/experiments/ai-positional-trader')
from config import DB_PATH, connect_db

def backfill_weekly(conn, symbols):
    print("Starting weekly data backfill (10 years)...")
    count = 0
    for symbol in symbols:
        ns_symbol = f"{symbol}.NS"
        print(f"Fetching weekly data for {ns_symbol}...")
        
        retries = 3
        delay = 2
        success = False
        
        for attempt in range(retries):
            try:
                ticker = yf.Ticker(ns_symbol)
                # Fetch 10 years of weekly data
                df = ticker.history(period="10y", interval="1wk")
                
                if not df.empty:
                    # Prepare data for insertion
                    df = df.reset_index()
                    df['symbol'] = symbol
                    
                    # Insert into DB
                    for _, row in df.iterrows():
                        date_str = row['Date'].strftime('%Y-%m-%d')
                        conn.execute("""
                            INSERT OR IGNORE INTO weekly_prices (symbol, date, open, high, low, close, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (symbol, date_str, row['Open'], row['High'], row['Low'], row['Close'], int(row['Volume'])))
                    
                    print(f"Saved weekly data for {symbol}")
                    success = True
                    break
                else:
                    print(f"Warning: No weekly data found for {ns_symbol}")
                    break
                    
            except Exception as e:
                print(f"Error fetching weekly for {ns_symbol}: {e}")
                if "429" in str(e) or "Too Many Requests" in str(e):
                    time.sleep(delay * 2)
                    delay *= 2
                else:
                    time.sleep(delay)
                    
        count += 1
        if count % 10 == 0:
            print(f"Processed {count}/{len(symbols)} symbols (Weekly).")
            
        time.sleep(1.5) # Polite delay
        
    print("Weekly backfill completed.")

def backfill_daily(conn, symbols):
    print("Starting daily data backfill (4 years)...")
    count = 0
    for symbol in symbols:
        ns_symbol = f"{symbol}.NS"
        print(f"Fetching daily data for {ns_symbol}...")
        
        retries = 3
        delay = 2
        success = False
        
        for attempt in range(retries):
            try:
                ticker = yf.Ticker(ns_symbol)
                # Fetch 4 years of daily data
                df = ticker.history(period="4y", interval="1d")
                
                if not df.empty:
                    # Prepare data for insertion
                    df = df.reset_index()
                    df['symbol'] = symbol
                    
                    # Insert into DB
                    for _, row in df.iterrows():
                        date_str = row['Date'].strftime('%Y-%m-%d')
                        conn.execute("""
                            INSERT OR IGNORE INTO prices (symbol, date, open, high, low, close, volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (symbol, date_str, row['Open'], row['High'], row['Low'], row['Close'], int(row['Volume'])))
                    
                    print(f"Saved daily data for {symbol}")
                    success = True
                    break
                else:
                    print(f"Warning: No daily data found for {ns_symbol}")
                    break
                    
            except Exception as e:
                print(f"Error fetching daily for {ns_symbol}: {e}")
                if "429" in str(e) or "Too Many Requests" in str(e):
                    time.sleep(delay * 2)
                    delay *= 2
                else:
                    time.sleep(delay)
                    
        count += 1
        if count % 10 == 0:
            print(f"Processed {count}/{len(symbols)} symbols (Daily).")
            
        time.sleep(1.5) # Polite delay
        
    print("Daily backfill completed.")

def main():
    print(f"Connecting to DB at {DB_PATH}")
    conn = connect_db(DB_PATH)
    
    # Get all symbols
    symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    print(f"Found {len(symbols)} symbols in universe.")
    
    # Run sequentially
    backfill_weekly(conn, symbols)
    backfill_daily(conn, symbols)
    
    conn.close()
    print("All historical data backfills completed.")

if __name__ == "__main__":
    main()
