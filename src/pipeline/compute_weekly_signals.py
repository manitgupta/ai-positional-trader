import os
import pandas as pd
import pandas_ta as ta
import duckdb
import sys
import datetime
from dotenv import load_dotenv

load_dotenv()

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import DB_PATH, connect_db

class WeeklySignalComputer:
    def __init__(self, db_path):
        self.db_path = db_path

    def load_weekly_prices(self, symbol):
        """Load weekly prices from DB for a symbol."""
        conn = connect_db(self.db_path)
        try:
            query = f"SELECT * FROM weekly_prices WHERE symbol = '{symbol}' ORDER BY date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading weekly prices for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def load_nifty_weekly(self):
        """Load Nifty 50 weekly prices."""
        conn = connect_db(self.db_path)
        try:
            query = "SELECT date, close FROM weekly_prices WHERE symbol = '^NSEI' ORDER BY date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading Nifty weekly prices: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def load_weekly_prices_batch(self, symbols):
        """Load weekly prices from DB for a list of symbols."""
        if not symbols:
            return pd.DataFrame()
        conn = connect_db(self.db_path)
        try:
            symbols_str = "', '".join(symbols)
            query = f"SELECT * FROM weekly_prices WHERE symbol IN ('{symbols_str}') ORDER BY symbol, date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading batch weekly prices: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def compute_signals(self, df, nifty_df):
        """Compute weekly signals."""
        if df.empty or len(df) < 30:
            print(f"Not enough data to compute weekly signals (need at least 30 rows, got {len(df)})")
            return pd.DataFrame()

        df = df.set_index(pd.to_datetime(df['date']))
        
        # SMAs
        df['sma_10'] = ta.sma(df['close'], length=10)
        df['sma_30'] = ta.sma(df['close'], length=30)
        
        # RSI
        df['rsi_14'] = ta.rsi(df['close'], length=14)
        
        # Volume Ratio (10-week)
        df['vol_sma_10'] = ta.sma(df['volume'], length=10)
        df['volume_ratio_10w'] = df['volume'] / df['vol_sma_10']
        
        # Mansfield RS
        if not nifty_df.empty:
            nifty_df = nifty_df.set_index(pd.to_datetime(nifty_df['date']))
            # Align by date
            merged = df.join(nifty_df, rsuffix='_nifty', how='inner')
            
            ratio = merged['close'] / merged['close_nifty']
            # 52-week MA of ratio
            ratio_ma = ratio.rolling(window=52).mean()
            
            # Mansfield formula: ((Ratio / MA of Ratio) - 1) * 10
            merged['mansfield_rs'] = ((ratio / ratio_ma) - 1) * 10
            
            df['mansfield_rs'] = merged['mansfield_rs']
        else:
            df['mansfield_rs'] = None

        df = df.reset_index(drop=True)
        
        # Select columns
        result_df = df[['symbol', 'date', 'sma_10', 'sma_30', 'rsi_14', 'volume_ratio_10w', 'mansfield_rs']]
        
        # Drop NaN rows for critical indicators
        result_df = result_df.dropna(subset=['sma_30'])
        
        return result_df

    def save_signals(self, df):
        """Save weekly signals to DB using ON CONFLICT DO UPDATE."""
        if df.empty:
            return
            
        conn = connect_db(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT INTO weekly_signals 
                SELECT symbol, date, sma_10, sma_30, rsi_14, volume_ratio_10w, mansfield_rs
                FROM df_view
                ON CONFLICT(symbol, date) DO UPDATE SET
                    sma_10           = excluded.sma_10,
                    sma_30           = excluded.sma_30,
                    rsi_14           = excluded.rsi_14,
                    volume_ratio_10w = excluded.volume_ratio_10w,
                    mansfield_rs     = excluded.mansfield_rs
            """)
            print(f"Saved {len(df)} total weekly signal rows to DB.")
        except Exception as e:
            print(f"Error saving weekly signals: {e}")
        finally:
            conn.close()

    def update_incremental(self):
        """Find symbols needing update, load recent price data, and compute/save signals."""
        print("Checking which weekly signals need update...")
        conn = connect_db(self.db_path)
        try:
            to_update_df = conn.execute("""
                SELECT p.symbol, MAX(s.date) AS max_signal_date
                FROM weekly_prices p
                LEFT JOIN weekly_signals s ON p.symbol = s.symbol
                GROUP BY p.symbol
                HAVING MAX(p.date) > MAX(s.date) OR MAX(s.date) IS NULL
            """).fetchdf()
        except Exception as e:
            print(f"Error checking incremental status: {e}")
            to_update_df = pd.DataFrame()
        finally:
            conn.close()

        if to_update_df.empty:
            print("Weekly signals are already up to date.")
            return
            
        symbols_to_update = to_update_df['symbol'].tolist()
        print(f"{len(symbols_to_update)} symbols need weekly signal computation.")
        
        print("Loading Nifty weekly benchmark...")
        nifty_df = self.load_nifty_weekly()
        
        all_weekly_signals = []
        
        chunk_size = 100
        for i in range(0, len(symbols_to_update), chunk_size):
            chunk = symbols_to_update[i:i+chunk_size]
            symbols_str = "', '".join(chunk)
            
            conn = connect_db(self.db_path)
            try:
                # Load last ~80 weeks using ROW_NUMBER for partition-based limit
                query = f"""
                    WITH numbered AS (
                        SELECT *, ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY date DESC) as rn
                        FROM weekly_prices
                        WHERE symbol IN ('{symbols_str}')
                    )
                    SELECT * FROM numbered WHERE rn <= 80 ORDER BY symbol, date
                """
                batch_prices = conn.execute(query).fetchdf()
            except Exception as e:
                print(f"Error fetching prices for chunk: {e}")
                batch_prices = pd.DataFrame()
            finally:
                conn.close()
                
            if batch_prices.empty:
                continue
                
            for symbol, df in batch_prices.groupby('symbol'):
                df = df.sort_values('date')
                
                # Look up the max signal date for this symbol
                meta = to_update_df[to_update_df['symbol'] == symbol]
                if meta.empty:
                    continue
                last_date = meta.iloc[0]['max_signal_date']
                
                if pd.notna(last_date):
                    if isinstance(last_date, str):
                        last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
                    elif isinstance(last_date, datetime.datetime):
                        last_date = last_date.date()
                
                signals_df = self.compute_signals(df, nifty_df)
                
                if not signals_df.empty:
                    if pd.notna(last_date):
                        # Keep only rows newer than max_signal_date
                        signals_df['date_obj'] = pd.to_datetime(signals_df['date']).dt.date
                        signals_df = signals_df[signals_df['date_obj'] > last_date]
                        signals_df = signals_df.drop(columns=['date_obj'])
                        
                    if not signals_df.empty:
                        all_weekly_signals.append(signals_df)
                        
        if all_weekly_signals:
            combined_df = pd.concat(all_weekly_signals, ignore_index=True)
            print(f"Saving {len(combined_df)} new calculated weekly signal rows...")
            self.save_signals(combined_df)
        else:
            print("No new weekly signals to save.")

def run_weekly_pipeline():
    computer = WeeklySignalComputer(DB_PATH)
    computer.update_incremental()

if __name__ == "__main__":
    run_weekly_pipeline()
