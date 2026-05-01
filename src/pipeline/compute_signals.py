import os
import pandas as pd
import pandas_ta as ta
import duckdb
from dotenv import load_dotenv
from config import connect_db

load_dotenv()

class SignalComputer:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_last_signal_date(self, symbol):
        """Get the latest date stored in the signals table for a symbol."""
        conn = connect_db(self.db_path)
        try:
            res = conn.execute("SELECT MAX(date) FROM signals WHERE symbol = ?", (symbol,)).fetchone()
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
        finally:
            conn.close()

    def load_prices(self, symbol, start_date=None):
        """Load prices from DB for a symbol."""
        conn = connect_db(self.db_path)
        try:
            if start_date:
                query = f"SELECT * FROM prices WHERE symbol = '{symbol}' AND date >= '{start_date}' ORDER BY date"
            else:
                query = f"SELECT * FROM prices WHERE symbol = '{symbol}' ORDER BY date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading prices for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def load_prices_batch(self, symbols, start_date=None):
        """Load prices from DB for a list of symbols."""
        if not symbols:
            return pd.DataFrame()
        conn = connect_db(self.db_path)
        try:
            symbols_str = "', '".join(symbols)
            if start_date:
                query = f"SELECT * FROM prices WHERE symbol IN ('{symbols_str}') AND date >= '{start_date}' ORDER BY symbol, date"
            else:
                query = f"SELECT * FROM prices WHERE symbol IN ('{symbols_str}') ORDER BY symbol, date"
            df = conn.execute(query).fetchdf()
            return df
        except Exception as e:
            print(f"Error loading batch prices: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def compute_signals(self, df, nifty_df=None):
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
        
        # Bollinger Bands Width
        bb = ta.bbands(df['close'], length=20, std=2)
        if bb is not None:
            bbb_col = [c for c in bb.columns if 'BBB' in c]
            if bbb_col:
                df['bb_width'] = bb[bbb_col[0]]
            else:
                df['bb_width'] = None
        else:
            df['bb_width'] = None
            
        # Daily RS vs Nifty
        if nifty_df is None:
            nifty_df = self.load_prices("^NSEI")
            
        if not nifty_df.empty:
            nifty_df = nifty_df.set_index(pd.to_datetime(nifty_df['date']))
            rs_ratio = df['close'] / nifty_df['close']
            df['daily_rs'] = ta.sma(rs_ratio, length=20) # 20-day smoothed ratio
        else:
            df['daily_rs'] = None
            
        # Calculate 12 month momentum for RS Rank
        df['close_shift_252'] = df['close'].shift(252)
        df['close_shift_252'] = df['close_shift_252'].fillna(df['close'].iloc[0])
        df['raw_momentum_12m'] = (df['close'] - df['close_shift_252']) / df['close_shift_252'] * 100
        
        df['rs_rank'] = 50 # Will be populated globally later
        
        # Reset index to get date column back
        df = df.reset_index(drop=True)
        
        # Select columns matching schema
        result_df = df[['symbol', 'date', 'rsi_14', 'adx_14', 'atr_14', 'macd_hist', 
                        'sma_50', 'sma_150', 'sma_200', 'above_200ma', 'rs_rank', 
                        'raw_momentum_12m', 'pct_from_52w_high', 'volume_ratio_20d',
                        'bb_width', 'daily_rs']]
        
        # Drop rows where critical signals are NaN (e.g. due to MA lag)
        result_df = result_df.dropna(subset=['sma_200'])
        
        return result_df

    def save_signals(self, df):
        """Save signals to DB."""
        if df.empty:
            print("No signals to save.")
            return
            
        conn = connect_db(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT INTO signals 
                SELECT symbol, date, rsi_14, adx_14, atr_14, macd_hist, 
                       sma_50, sma_150, sma_200, above_200ma, rs_rank, 
                       raw_momentum_12m, pct_from_52w_high, volume_ratio_20d,
                       bb_width, daily_rs
                FROM df_view
                ON CONFLICT(symbol, date) DO NOTHING
            """)
            print(f"Saved {len(df)} signal rows to DB.")
        except Exception as e:
            print(f"Error saving signals to DB: {e}")
        finally:
            conn.close()

