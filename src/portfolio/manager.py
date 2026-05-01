import os
import datetime
import duckdb
from config import connect_db

class PortfolioManager:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def open_position(self, symbol, entry_price, quantity, stop_loss, target, position_pct, thesis_summary, entry_date=None):
        conn = connect_db(self.db_path)
        try:
            if entry_date is None:
                entry_date = datetime.date.today()
            conn.execute("""
                INSERT OR REPLACE INTO portfolio (symbol, entry_date, entry_price, quantity, stop_loss, target, position_pct, thesis_summary, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
            """, (symbol, entry_date, entry_price, quantity, stop_loss, target, position_pct, thesis_summary))
            print(f"Opened position for {symbol}")
        except Exception as e:
            print(f"Error opening position: {e}")
        finally:
            conn.close()
            
    def close_position(self, symbol, exit_price, exit_date=None):
        conn = connect_db(self.db_path)
        try:
            if exit_date is None:
                exit_date = datetime.date.today()
            conn.execute("""
                UPDATE portfolio 
                SET status = 'CLOSED', exit_price = ?, exit_date = ? 
                WHERE symbol = ? AND status = 'OPEN'
            """, (exit_price, exit_date, symbol))
            print(f"Closed position for {symbol} at {exit_price}")
        except Exception as e:
            print(f"Error closing position: {e}")
        finally:
            conn.close()
            
    def update_stop_loss(self, symbol, new_stop):
        conn = connect_db(self.db_path)
        try:
            conn.execute("""
                UPDATE portfolio SET stop_loss = ? WHERE symbol = ? AND status = 'OPEN'
            """, (new_stop, symbol))
            print(f"Updated stop loss for {symbol} to {new_stop}")
        except Exception as e:
            print(f"Error updating stop loss: {e}")
        finally:
            conn.close()
            
    def get_open_positions(self):
        conn = connect_db(self.db_path)
        try:
            return conn.execute("SELECT * FROM portfolio WHERE status = 'OPEN'").fetchdf()
        except Exception as e:
            print(f"Error fetching portfolio: {e}")
            return None
        finally:
            conn.close()

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    base_dir = os.path.dirname(src_dir)
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    
    manager = PortfolioManager(db_path)
    manager.open_position("RELIANCE", 2500.0, 10, 2400.0, 2800.0, 10.0, "Test thesis")
    
    positions = manager.get_open_positions()
    print(positions)
    
    manager.update_stop_loss("RELIANCE", 2450.0)
    manager.close_position("RELIANCE")
