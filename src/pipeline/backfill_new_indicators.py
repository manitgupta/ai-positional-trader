import os
import pandas as pd
import duckdb
import sys
import time

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import DB_PATH
from src.pipeline.compute_signals import SignalComputer

def backfill_daily_signals():
    computer = SignalComputer(DB_PATH)
    
    # Get all unique symbols from universe
    conn = duckdb.connect(DB_PATH)
    symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    conn.close()
    
    for symbol in symbols:
        print(f"Backfilling daily signals for {symbol}...")
        df = computer.load_prices(symbol)
        if not df.empty:
            signals_df = computer.compute_signals(df)
            
            if not signals_df.empty:
                conn = duckdb.connect(DB_PATH)
                try:
                    conn.register('df_view', signals_df)
                    # Update existing rows with new indicators
                    conn.execute("""
                        UPDATE signals 
                        SET bb_width = df_view.bb_width, 
                            daily_rs = df_view.daily_rs
                        FROM df_view
                        WHERE signals.symbol = df_view.symbol AND signals.date = df_view.date
                    """)
                    print(f"Updated rows for {symbol}")
                except Exception as e:
                    print(f"Error updating signals for {symbol}: {e}")
                finally:
                    conn.close()
        
        time.sleep(0.5) # Throttling

if __name__ == "__main__":
    backfill_daily_signals()
