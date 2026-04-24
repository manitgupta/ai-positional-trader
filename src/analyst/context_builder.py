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
                SELECT p.*, u.company_name, pr.close as current_price, s.rsi_14, s.adx_14,
                       round((pr.close - p.entry_price) / p.entry_price * 100, 1) as pnl_pct,
                       round(p.quantity * p.entry_price, 2) as total_buy_price
                FROM portfolio p
                LEFT JOIN universe u ON p.symbol = u.symbol
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

    def get_capital_state(self):
        conn = duckdb.connect(self.db_path)
        try:
            # Get total capital
            total_capital_res = conn.execute("SELECT total_capital FROM account LIMIT 1").fetchone()
            total_capital = total_capital_res[0] if total_capital_res else 1000000.0
            
            # Get invested amount (OPEN positions)
            invested_res = conn.execute("SELECT sum(entry_price * quantity) FROM portfolio WHERE status = 'OPEN'").fetchone()
            invested_amount = invested_res[0] if invested_res and invested_res[0] is not None else 0.0
            
            # Get realized PnL (CLOSED positions)
            realized_res = conn.execute("SELECT sum((exit_price - entry_price) * quantity) FROM portfolio WHERE status = 'CLOSED'").fetchone()
            realized_pnl = realized_res[0] if realized_res and realized_res[0] is not None else 0.0
            
            available_cash = total_capital + realized_pnl - invested_amount
            
            return {
                'total_capital': total_capital,
                'invested_amount': invested_amount,
                'realized_pnl': realized_pnl,
                'available_cash': available_cash
            }
        except Exception as e:
            print(f"Error getting capital state: {e}")
            return {
                'total_capital': 1000000.0,
                'invested_amount': 0.0,
                'realized_pnl': 0.0,
                'available_cash': 1000000.0
            }
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
            
        portfolio_text = ""
        if not open_positions.empty:
            conn = duckdb.connect(self.db_path)
            try:
                for index, row in open_positions.iterrows():
                    symbol = row['symbol']
                    entry_date = row['entry_date']
                    
                    # History around buy time
                    buy_query = f"""
                        SELECT s.date, p.close, p.volume, s.rsi_14, s.adx_14
                        FROM signals s
                        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
                        WHERE s.symbol = '{symbol}' AND s.date >= CAST('{entry_date}' AS DATE) - INTERVAL 15 DAY AND s.date <= CAST('{entry_date}' AS DATE) + INTERVAL 15 DAY
                        ORDER BY s.date ASC
                    """
                    buy_history = conn.execute(buy_query).fetchdf()
                    portfolio_text += f"\n--- {symbol} (Around Buy Time {entry_date}) ---\n"
                    portfolio_text += buy_history.to_string(index=False) + "\n"
                    
                    # Latest history
                    latest_query = f"""
                        SELECT s.date, p.close, p.volume, s.rsi_14, s.adx_14
                        FROM signals s
                        JOIN prices p ON s.symbol = p.symbol AND s.date = p.date
                        WHERE s.symbol = '{symbol}'
                        ORDER BY s.date DESC
                        LIMIT 10
                    """
                    latest_history = conn.execute(latest_query).fetchdf()
                    portfolio_text += f"\n--- {symbol} (Latest 10 Days) ---\n"
                    portfolio_text += latest_history.to_string(index=False) + "\n"
            except Exception as e:
                print(f"Error fetching history for portfolio: {e}")
                portfolio_text += f"\nError fetching history for {symbol}\n"
            finally:
                conn.close()
        else:
            portfolio_text = "No open positions."
        
        capital_state = self.get_capital_state()
        
        context = f"""
## Macro backdrop ({today})
{macro}

## Capital State
Total Capital: {capital_state['total_capital']}
Invested Amount: {capital_state['invested_amount']}
Realized PnL: {capital_state['realized_pnl']}
Available Cash: {capital_state['available_cash']}

## Your open portfolio
{open_positions.to_string() if not open_positions.empty else "No open positions."}

## Portfolio History (Around Buy Time & Latest)
{portfolio_text}

## Your research notes — last 45 days
{prior_notes.to_string() if not prior_notes.empty else "No prior research notes."}

## Today's candidates snapshot (with fundamentals)
{candidates_df.to_string(index=False) if not candidates_df.empty else "No candidates passed filters today."}

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
