import os
import datetime
import time
import argparse
import duckdb
import sys

# Add base directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from config import DB_PATH, connect_db
from src.pipeline.fetch_delivery import DeliveryFetcher

def run_backfill(days_lookback=1460):
    print(f"Starting historical backfill of delivery data for the last {days_lookback} days.")
    
    fetcher = DeliveryFetcher(DB_PATH)
    
    # 1. Find existing dates in DB to avoid re-fetching
    conn = connect_db(DB_PATH)
    try:
        existing_dates = set(r[0] for r in conn.execute("SELECT DISTINCT date FROM delivery_data").fetchall())
    finally:
        conn.close()
    
    print(f"Identified {len(existing_dates)} dates already present in database.")
    
    # 2. Compute target dates range
    today = datetime.date.today()
    target_dates = []
    
    for i in range(1, days_lookback + 1):
        d = today - datetime.timedelta(days=i)
        
        # Skip weekends immediately to optimize speed
        if d.weekday() >= 5: # Saturday and Sunday
            continue
            
        # Skip if already loaded
        if d in existing_dates:
            continue
            
        target_dates.append(d)
    
    print(f"Found {len(target_dates)} weekday dates missing from database.")
    
    if not target_dates:
        print("All missing data caught up! Exiting.")
        return
    
    # Sort descending to load recent dates first (optional, but preferred)
    target_dates.sort(reverse=True)
    
    success_count = 0
    fail_count = 0
    
    for idx, dt in enumerate(target_dates):
        print(f"\n[{idx+1}/{len(target_dates)}] Backfilling {dt}...")
        
        success = fetcher.fetch_for_date(dt)
        
        if success:
            success_count += 1
        else:
            fail_count += 1
            
        # Minor pause to avoid overwhelming servers, although 'nse' library enforces its own throttle.
        time.sleep(0.5)
        
        # Log periodic progress
        if (idx + 1) % 20 == 0:
             print(f"Progress summary: {success_count} days imported successfully, {fail_count} skipped/failed.")

    print("\n" + "="*40)
    print(f"Historical Backfill Completed.")
    print(f"Successfully processed: {success_count} days.")
    print(f"Skipped/Failed: {fail_count} days.")
    print("="*40)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill NSE delivery data.")
    parser.add_argument('--days', type=int, default=1460, help="Lookback window in days (default 1460 = 4 years).")
    args = parser.parse_args()
    
    run_backfill(days_lookback=args.days)
