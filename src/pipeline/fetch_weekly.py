import os
import datetime
import pandas as pd
import yfinance as yf
import duckdb
from config import connect_db

class WeeklyPriceFetcher:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def get_last_weekly_date(self, conn):
        try:
            res = conn.execute("SELECT MAX(date) FROM weekly_prices").fetchone()
            if res and res[0]:
                import datetime
                if isinstance(res[0], str):
                    return datetime.datetime.strptime(res[0], "%Y-%m-%d").date()
                elif isinstance(res[0], datetime.datetime):
                    return res[0].date()
                elif isinstance(res[0], datetime.date):
                    return res[0]
            return None
        except Exception:
            return None
            
    def fetch_batch_weekly_data(self, symbols, chunk_size=100):
        """Fetch weekly data for a list of symbols from Yahoo Finance in chunks."""
        from src.pipeline.weekly_calendar import last_closed_week_monday
        expected_last = last_closed_week_monday()
        
        conn = connect_db(self.db_path)
        last_stored = self.get_last_weekly_date(conn)
        conn.close()
        
        if last_stored and last_stored >= expected_last:
            print(f"Weekly DB already current through {expected_last}. Skipping.")
            return pd.DataFrame()
            
        today = datetime.date.today()
        
        if last_stored:
            from_date = last_stored + datetime.timedelta(days=1)
        else:
            from_date = today - datetime.timedelta(days=365)
            
        print(f"Incremental Weekly Fetch: Downloading from {from_date} through expected last {expected_last}...")
        
        all_data = []
        
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            tickers = [f"{s}.NS" if not s.endswith('.NS') and not s.endswith('.BO') else s for s in chunk]
            tickers_str = " ".join(tickers)
            
            print(f"Fetching weekly batch {i // chunk_size + 1} ({len(chunk)} tickers)...")
            try:
                import time
                time.sleep(2) # Prevent rate limiting
                df = yf.download(tickers_str, start=from_date, end=today + datetime.timedelta(days=1), interval="1wk", progress=False, group_by='column')
                
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
                    
                    # Filter out any partial in-progress week
                    df = df[df['date'] <= expected_last]
                    
                    if not df.empty:
                        all_data.append(df)
                    
            except Exception as e:
                print(f"Error fetching weekly chunk starting at {i}: {e}")
                
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def save_to_db(self, df):
        """Save DataFrame to DuckDB."""
        if df.empty:
            print("No weekly data to save.")
            return
            
        conn = connect_db(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT INTO weekly_prices 
                SELECT symbol, date, open, high, low, close, volume 
                FROM df_view
                ON CONFLICT(symbol, date) DO UPDATE SET 
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume
            """)
            print(f"Saved {len(df)} weekly price rows to DB.")
        except Exception as e:
            print(f"Error saving weekly prices to DB: {e}")
        finally:
            conn.close()
