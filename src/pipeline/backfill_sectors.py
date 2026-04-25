import duckdb
import yfinance as yf
import time
import os
import sys

# Add project root to path to import config
sys.path.append('/Users/manitgupta/experiments/ai-positional-trader')
from config import DB_PATH

def backfill_sectors():
    print(f"Connecting to DB at {DB_PATH}")
    conn = duckdb.connect(DB_PATH)
    
    # Find symbols that need backfilling
    symbols = conn.execute("""
        SELECT symbol FROM universe 
        WHERE sector IS NULL OR industry IS NULL
    """).fetchdf()['symbol'].tolist()
    
    print(f"Found {len(symbols)} symbols to backfill.")
    
    count = 0
    for symbol in symbols:
        ns_symbol = f"{symbol}.NS"
        print(f"Fetching data for {ns_symbol}...")
        
        retries = 3
        delay = 2
        success = False
        
        for attempt in range(retries):
            try:
                ticker = yf.Ticker(ns_symbol)
                info = ticker.info
                
                sector = info.get('sector')
                industry = info.get('industry')
                
                if sector and industry:
                    conn.execute("""
                        UPDATE universe 
                        SET sector = ?, industry = ? 
                        WHERE symbol = ?
                    """, (sector, industry, symbol))
                    print(f"Updated {symbol}: Sector='{sector}', Industry='{industry}'")
                    success = True
                    break
                else:
                    print(f"Warning: No sector/industry found for {ns_symbol} on attempt {attempt+1}")
                    # Some symbols might not have it, don't retry indefinitely
                    break
                    
            except Exception as e:
                print(f"Error fetching {ns_symbol} on attempt {attempt+1}: {e}")
                if "429" in str(e) or "Too Many Requests" in str(e):
                    print(f"Rate limit hit. Sleeping for {delay*2}s...")
                    time.sleep(delay * 2)
                    delay *= 2
                else:
                    time.sleep(delay)
                    
        if not success:
            print(f"Failed to update {symbol} after {retries} attempts or missing data.")
            
        count += 1
        if count % 10 == 0:
            print(f"Processed {count}/{len(symbols)} symbols.")
            
        # Polite delay between requests
        time.sleep(1.5)
        
    conn.close()
    print("Backfill completed.")

if __name__ == "__main__":
    backfill_sectors()
