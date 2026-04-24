import os
import datetime
import pandas as pd
import duckdb

class ContextBuilder:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def get_macro_snapshot(self):
        # Mock macro data for now
        return """
Nifty 50: Holding above 200 MA, in Stage 2 uptrend.
VIX: 14.5 (Normal).
FII Flows: Net buyers last 5 days.
Leading Sectors: IT, Auto, Capital Goods.
"""

    def get_open_positions(self):
        conn = duckdb.connect(self.db_path)
        try:
            # Check if portfolio table exists and has data
            table_check = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'portfolio'").fetchone()[0]
            if table_check == 0:
                return pd.DataFrame()
                
            query = """
                SELECT p.*, pr.close as current_price, s.rsi_14, s.adx_14,
                       round((pr.close - p.entry_price) / p.entry_price * 100, 1) as pnl_pct
                FROM portfolio p
                LEFT JOIN signals s ON p.symbol = s.symbol
                LEFT JOIN prices pr ON p.symbol = pr.symbol AND s.date = pr.date
                WHERE p.status = 'OPEN'
                QUALIFY ROW_NUMBER() OVER(PARTITION BY p.symbol ORDER BY s.date DESC) = 1
            """
            return conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Error fetching open positions: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def get_prior_notes(self):
        conn = duckdb.connect(self.db_path)
        try:
            table_check = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'research_journal'").fetchone()[0]
            if table_check == 0:
                return pd.DataFrame()
                
            query = """
                SELECT symbol, date, thesis, conviction, status, entry_trigger
                FROM research_journal
                WHERE date > current_date - INTERVAL 45 DAY
                ORDER BY symbol, date DESC
            """
            return conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Error fetching prior notes: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def build_context(self, candidates_df):
        macro = self.get_macro_snapshot()
        open_positions = self.get_open_positions()
        prior_notes = self.get_prior_notes()
        
        today = datetime.date.today().strftime("%Y-%m-%d")
        
        candidates_text = ""
        if not candidates_df.empty:
            conn = duckdb.connect(self.db_path)
            try:
                for symbol in candidates_df['symbol'].tolist():
                    query = f"""
                        SELECT s.date, p.close, p.volume, s.rsi_14, s.adx_14, s.volume_ratio_20d
                        FROM signals s
                        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
                        WHERE s.symbol = '{symbol}'
                        ORDER BY s.date DESC
                        LIMIT 30
                    """
                    history = conn.execute(query).fetchdf()
                    candidates_text += f"\n--- {symbol} (Last 30 Days) ---\n"
                    candidates_text += history.to_string(index=False) + "\n"
                    
                    # Fetch last 5 weekly candles
                    weekly_query = f"""
                        SELECT date, open, high, low, close, volume
                        FROM weekly_prices
                        WHERE symbol = '{symbol}'
                        ORDER BY date DESC
                        LIMIT 5
                    """
                    weekly_history = conn.execute(weekly_query).fetchdf()
                    candidates_text += f"\n--- {symbol} (Last 5 Weeks) ---\n"
                    candidates_text += weekly_history.to_string(index=False) + "\n"
            except Exception as e:
                print(f"Error fetching history for candidates: {e}")
                candidates_text += f"\nError fetching history for {symbol}\n"
            finally:
                conn.close()
        else:
            candidates_text = "No candidates passed filters today."
        
        context = f"""
## Macro backdrop ({today})
{macro}

## Your open portfolio
{open_positions.to_string() if not open_positions.empty else "No open positions."}

## Your research notes — last 45 days
{prior_notes.to_string() if not prior_notes.empty else "No prior research notes."}

## Today's screener candidates (with last 30 days history)
{candidates_text}
"""
        return context

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    base_dir = os.path.dirname(src_dir)
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    
    builder = ContextBuilder(db_path)
    
    # Mock candidates for testing
    dummy_candidates = pd.DataFrame({
        'symbol': ['RELIANCE', 'TCS'],
        'close': [2500, 3500],
        'rs_rank': [85, 90],
        'composite_score': [0.8, 0.85]
    })
    
    context = builder.build_context(dummy_candidates)
    print(context)
