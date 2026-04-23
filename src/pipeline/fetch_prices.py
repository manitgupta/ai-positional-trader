import os
import datetime
import pandas as pd
import duckdb
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

class PriceFetcher:
    def __init__(self, db_path):
        self.db_path = db_path
        print("Using Yahoo Finance batch data source for prices.")
            
    def fetch_batch_eod_data(self, symbols, from_date, to_date, chunk_size=100):
        """Fetch EOD data for a list of symbols from Yahoo Finance in chunks."""
        print(f"Fetching Yahoo Finance data for {len(symbols)} symbols in chunks of {chunk_size}...")
        
        all_data = []
        
        # Chunk the symbols
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            tickers = [f"{s}.NS" if not s.endswith('.NS') and not s.endswith('.BO') else s for s in chunk]
            tickers_str = " ".join(tickers)
            
            print(f"Fetching batch {i // chunk_size + 1} ({len(chunk)} tickers)...")
            try:
                import time
                time.sleep(2)
                df = yf.download(tickers_str, start=from_date, end=to_date, progress=False, group_by='column')
                
                if not df.empty:
                    # Handle MultiIndex if multiple tickers returned
                    if isinstance(df.columns, pd.MultiIndex):
                        df = df.stack(level=1).reset_index()
                        # Rename columns to lowercase
                        df.columns = [col.lower() for col in df.columns]
                        # Rename 'level_1' or 'ticker' if present to symbol
                        if 'level_1' in df.columns:
                            df = df.rename(columns={'level_1': 'symbol'})
                        elif 'ticker' in df.columns:
                            df = df.rename(columns={'ticker': 'symbol'})
                    else:
                        # Single ticker returned
                        df = df.reset_index()
                        df['symbol'] = chunk[0]
                        df.columns = [col.lower() for col in df.columns]
                        
                    # Ensure symbol removes .NS suffix for DB storage consistency
                    df['symbol'] = df['symbol'].astype(str).str.replace('.NS', '', regex=False)
                    
                    # Map Date to date
                    if 'date' not in df.columns and 'datetime' in df.columns:
                        df['date'] = df['datetime']
                        
                    df['date'] = pd.to_datetime(df['date']).dt.date
                    
                    # Select columns matching schema
                    df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
                    df = df.dropna(subset=['open', 'high', 'low', 'close'])
                    
                    all_data.append(df)
                    
            except Exception as e:
                print(f"Error fetching chunk starting at {i}: {e}")
                
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def save_to_db(self, df):
        """Save DataFrame to DuckDB."""
        if df.empty:
            print("No data to save.")
            return
            
        conn = duckdb.connect(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR REPLACE INTO prices 
                SELECT symbol, date, open, high, low, close, volume 
                FROM df_view
            """)
            print(f"Saved {len(df)} rows to DB.")
        except Exception as e:
            print(f"Error saving to DB: {e}")
        finally:
            conn.close()

