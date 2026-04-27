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
        
    def get_last_updated_date(self, conn):
        """Get the latest date stored in the prices table."""
        try:
            res = conn.execute("SELECT MAX(date) FROM prices").fetchone()
            if res and res[0]:
                # Convert to date object if it's a string or datetime
                if isinstance(res[0], str):
                    return datetime.datetime.strptime(res[0], "%Y-%m-%d").date()
                elif isinstance(res[0], datetime.datetime):
                    return res[0].date()
                elif isinstance(res[0], datetime.date):
                    return res[0]
            return None
        except Exception:
            return None

    def fetch_batch_eod_data(self, symbols, from_date, to_date, chunk_size=100):
        """Fetch EOD data for a list of symbols from Yahoo Finance in chunks."""
        conn = duckdb.connect(self.db_path)
        last_date = self.get_last_updated_date(conn)
        conn.close()
        
        # Incremental Update Optimization:
        # If we already have data, start fetching from the day after the last date
        if last_date:
            from_date = last_date + datetime.timedelta(days=1)
            
        if from_date >= to_date:
            print("Database is already up to date. Skipping price fetch.")
            return pd.DataFrame()
            
        print(f"Incremental Fetch: Downloading prices from {from_date} to {to_date}...")
        print(f"Fetching Yahoo Finance data for {len(symbols)} symbols in chunks of {chunk_size}...")
        
        all_data = []
        
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            tickers = [f"{s}.NS" if not s.endswith('.NS') and not s.endswith('.BO') else s for s in chunk]
            tickers_str = " ".join(tickers)
            
            print(f"Fetching batch {i // chunk_size + 1} ({len(chunk)} tickers)...")
            try:
                import time
                time.sleep(2) # Prevent rate limiting
                df = yf.download(tickers_str, start=from_date, end=to_date + datetime.timedelta(days=1), progress=False, group_by='column')
                
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
                    
                    all_data.append(df)
                    
            except Exception as e:
                print(f"Error fetching chunk starting at {i}: {e}")
                
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def save_to_db(self, df):
        """Save DataFrame to DuckDB."""
        if df.empty:
            return
            
        conn = duckdb.connect(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT INTO prices 
                SELECT symbol, date, open, high, low, close, volume 
                FROM df_view
                ON CONFLICT(symbol, date) DO UPDATE SET 
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume
            """)
            print(f"Saved {len(df)} incremental rows to DB.")
        except Exception as e:
            print(f"Error saving to DB: {e}")
        finally:
            conn.close()
