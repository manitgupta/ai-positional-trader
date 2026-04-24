import os
import sys
import csv
import datetime
import duckdb

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
base_dir = os.path.dirname(src_dir)
sys.path.append(base_dir)

from src.portfolio.manager import PortfolioManager
from config import DB_PATH

def process_trades(csv_path):
    print(f"Processing trades from {csv_path}")
    if not os.path.exists(csv_path):
        print(f"Error: File not found at {csv_path}")
        return
        
    manager = PortfolioManager(DB_PATH)
    
    with open(csv_path, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            symbol = row.get('symbol')
            action = row.get('action', '').upper()
            try:
                price = float(row.get('price', 0))
                quantity = int(row.get('quantity', 0))
            except ValueError:
                print(f"Skipping row due to invalid price or quantity: {row}")
                continue
                
            date_str = row.get('date')
            trade_date = None
            if date_str:
                try:
                    trade_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                except ValueError:
                    print(f"Invalid date format for {symbol}, using default.")
                
            thesis = row.get('thesis', '')
            stop_loss = float(row.get('stop_loss')) if row.get('stop_loss') else None
            target = float(row.get('target')) if row.get('target') else None
            position_pct = float(row.get('position_pct')) if row.get('position_pct') else None
            
            if action == 'BUY':
                print(f"Recording BUY for {symbol}...")
                manager.open_position(symbol, price, quantity, stop_loss, target, position_pct, thesis, entry_date=trade_date)
            elif action == 'SELL':
                print(f"Recording SELL for {symbol}...")
                manager.close_position(symbol, price, exit_date=trade_date)
            else:
                print(f"Unknown action {action} for {symbol}, skipping.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_portfolio.py <path_to_csv>")
    else:
        process_trades(sys.argv[1])
