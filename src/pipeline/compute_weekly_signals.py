import os
import pandas as pd
import pandas_ta as ta
import duckdb
import sys
from dotenv import load_dotenv

load_dotenv()

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import DB_PATH

class WeeklySignalComputer:
    def __init__(self, db_path):
        self.db_path = db_path

    def load_weekly_prices(self, symbol):
        """Load weekly prices from DB for a symbol."""
        conn = duckdb.connect(self.db_path)
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
        conn = duckdb.connect(self.db_path)
        try:
            query = "SELECT date, close FROM weekly_prices WHERE symbol = '^NSEI' ORDER BY date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading Nifty weekly prices: {e}")
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
        """Save weekly signals to DB."""
        if df.empty:
            return
            
        conn = duckdb.connect(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR IGNORE INTO weekly_signals 
                SELECT symbol, date, sma_10, sma_30, rsi_14, volume_ratio_10w, mansfield_rs
                FROM df_view
            """)
            print(f"Saved {len(df)} weekly signal rows for {df['symbol'].iloc[0]} to DB.")
        except Exception as e:
            print(f"Error saving weekly signals: {e}")
        finally:
            conn.close()

def run_weekly_pipeline():
    computer = WeeklySignalComputer(DB_PATH)
    
    # Get all unique symbols from universe
    conn = duckdb.connect(DB_PATH)
    symbols = conn.execute("SELECT symbol FROM universe").fetchdf()['symbol'].tolist()
    conn.close()
    
    nifty_df = computer.load_nifty_weekly()
    
    for symbol in symbols:
        print(f"Computing weekly signals for {symbol}...")
        df = computer.load_weekly_prices(symbol)
        if not df.empty:
            signals_df = computer.compute_signals(df, nifty_df)
            computer.save_signals(signals_df)

if __name__ == "__main__":
    run_weekly_pipeline()
