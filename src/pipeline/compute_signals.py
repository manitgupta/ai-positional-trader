import os
import pandas as pd
import pandas_ta as ta
import duckdb
from dotenv import load_dotenv

load_dotenv()

class SignalComputer:
    def __init__(self, db_path):
        self.db_path = db_path

    def load_prices(self, symbol):
        """Load prices from DB for a symbol."""
        conn = duckdb.connect(self.db_path)
        try:
            query = f"SELECT * FROM prices WHERE symbol = '{symbol}' ORDER BY date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading prices for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def compute_signals(self, df):
        """Compute signals using pandas-ta."""
        if df.empty or len(df) < 200:
            print(f"Not enough data to compute signals (need at least 200 rows, got {len(df)})")
            return pd.DataFrame()

        # Ensure date is datetime for pandas-ta if needed, or just use index
        df = df.set_index(pd.to_datetime(df['date']))
        
        # RSI
        df['rsi_14'] = ta.rsi(df['close'], length=14)
        
        # ADX
        adx = ta.adx(df['high'], df['low'], df['close'], length=14)
        if adx is not None:
            df['adx_14'] = adx['ADX_14']
        
        # ATR
        df['atr_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
        # MACD
        macd = ta.macd(df['close'])
        if macd is not None:
            # Columns usually MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9
            # We want the histogram
            hist_col = [c for c in macd.columns if 'MACDh' in c]
            if hist_col:
                df['macd_hist'] = macd[hist_col[0]]
        
        # SMAs
        df['sma_50'] = ta.sma(df['close'], length=50)
        df['sma_150'] = ta.sma(df['close'], length=150)
        df['sma_200'] = ta.sma(df['close'], length=200)
        
        # Derived
        df['above_200ma'] = df['close'] > df['sma_200']
        
        # 52 Week High (approx 252 trading days)
        df['52w_high'] = df['close'].rolling(window=252, min_periods=1).max()
        df['pct_from_52w_high'] = (df['close'] - df['52w_high']) / df['52w_high'] * 100
        
        # Volume Ratio
        df['vol_sma_20'] = ta.sma(df['volume'], length=20)
        df['volume_ratio_20d'] = df['volume'] / df['vol_sma_20']
        
        # RS Rank Placeholder
        # In a full implementation, this would compare returns against Nifty or other stocks
        # Here we just use a simple momentum score as a proxy for ranking later
        df['rs_rank'] = 50 # Default placeholder
        
        # Reset index to get date column back
        df = df.reset_index(drop=True)
        
        # Select columns matching schema
        result_df = df[['symbol', 'date', 'rsi_14', 'adx_14', 'atr_14', 'macd_hist', 
                        'sma_50', 'sma_150', 'sma_200', 'above_200ma', 'rs_rank', 
                        'pct_from_52w_high', 'volume_ratio_20d']]
        
        # Drop rows where critical signals are NaN (e.g. due to MA lag)
        result_df = result_df.dropna(subset=['sma_200'])
        
        return result_df

    def save_signals(self, df):
        """Save signals to DB."""
        if df.empty:
            print("No signals to save.")
            return
            
        conn = duckdb.connect(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR IGNORE INTO signals 
                SELECT symbol, date, rsi_14, adx_14, atr_14, macd_hist, 
                       sma_50, sma_150, sma_200, above_200ma, rs_rank, 
                       pct_from_52w_high, volume_ratio_20d 
                FROM df_view
            """)
            print(f"Saved {len(df)} signal rows to DB.")
        except Exception as e:
            print(f"Error saving signals to DB: {e}")
        finally:
            conn.close()

