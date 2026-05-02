import os
import sys
import json
import argparse
import datetime

import pandas as pd

# Path bootstrap
current_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(current_dir)
if base_dir not in sys.path:
    sys.path.append(base_dir)

from config import DB_PATH, connect_db

def backfill_symbol_state(conn) -> int:
    """Backfills symbol_state from research_journal event history."""
    
    # 1. Fetch all journal events in chronological order
    events = conn.execute("""
        SELECT symbol, date, thesis, conviction, status as raw_status, entry_trigger
        FROM research_journal
        ORDER BY symbol, date ASC
    """).fetchdf()
    
    if events.empty:
        return 0

    # Group events by symbol
    grouped = events.groupby('symbol')
    
    # Load existing portfolio symbols to aid status mapping
    try:
        portfolio_open = set(conn.execute("SELECT symbol FROM portfolio WHERE status = 'OPEN'").fetchdf()['symbol'].tolist())
    except:
        portfolio_open = set()
        
    today = datetime.date.today()
    count = 0

    for symbol, data in grouped:
        data = data.sort_values('date')
        
        first_seen = data.iloc[0]['date']
        if isinstance(first_seen, str): first_seen = datetime.datetime.strptime(first_seen, "%Y-%m-%d").date()
        
        last_row = data.iloc[-1]
        last_seen = last_row['date']
        if isinstance(last_seen, str): last_seen = datetime.datetime.strptime(last_seen, "%Y-%m-%d").date()
        
        days_tracked = (last_seen - first_seen).days
        
        # Build histories
        conviction_history = []
        status_history = []
        trigger_history = []
        
        prev_trigger = None
        trigger_static_days = 0
        last_t_date = None

        for _, row in data.iterrows():
            row_date = row['date']
            if isinstance(row_date, str): row_date = datetime.datetime.strptime(row_date, "%Y-%m-%d").date()
            d_str = str(row_date)
            
            if pd.notna(row['conviction']):
                conviction_history.append({"date": d_str, "conviction": int(row['conviction'])})
            
            # Status Mapping
            raw = str(row['raw_status']).upper() if row['raw_status'] else ""
            if raw == "ENTER":
                mapped = "ENTERED" if symbol in portfolio_open else "TRIGGERED"
            elif raw in ["HOLD", "TRAIL_STOP"]:
                mapped = "ENTERED"
            elif raw == "EXIT":
                mapped = "EXITED"
            elif raw in ["WATCH_FOR_ENTRY", "WATCHLIST_ENTRY", "WATCHLIST", "BUY_SETUP"]:
                mapped = "WATCHING"
            elif raw == "REMOVE_FROM_WATCHLIST":
                mapped = "REJECTED"
            else:
                mapped = "WATCHING"
            
            status_history.append({"date": d_str, "status": mapped})
            
            curr_t = row['entry_trigger']
            if curr_t and curr_t != prev_trigger:
                trigger_history.append({"date": d_str, "entry_trigger": curr_t})
                prev_trigger = curr_t
                # Reset when it changes
                trigger_static_days = 0
                last_t_date = row_date
            elif curr_t and curr_t == prev_trigger:
                if last_t_date:
                    delta = (row_date - last_t_date).days
                    trigger_static_days += max(delta, 1)
                else:
                    trigger_static_days += 1
                last_t_date = row_date

        conviction_history = conviction_history[-10:]
        status_history = status_history[-10:]
        trigger_history = trigger_history[-5:]

        # Final current values from latest row
        final_action = str(last_row['raw_status'])
        final_status = status_history[-1]['status']
        
        # Clean thesis
        final_thesis = str(last_row['thesis']).strip() if last_row['thesis'] else None

        params = (
            symbol,
            first_seen,
            last_seen,
            days_tracked,
            final_status,
            int(last_row['conviction']) if pd.notna(last_row['conviction']) else None,
            final_thesis,
            last_row['entry_trigger'],
            None, # Stop loss defaults to null in migration
            None, # Target defaults to null in migration
            json.dumps(conviction_history),
            json.dumps(status_history),
            json.dumps(trigger_history),
            trigger_static_days,
            None, # rejection_reason
            final_action,
            last_seen, # last_run_date
            datetime.datetime.now()
        )
        
        conn.execute("""
            INSERT OR REPLACE INTO symbol_state (
                symbol, first_seen_date, last_seen_date, days_tracked, current_status,
                current_conviction, current_thesis, current_entry_trigger, current_stop_loss, current_target,
                conviction_history, status_history, trigger_history, trigger_static_days,
                rejection_reason, last_action, last_run_date, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, params)
        count += 1

    return count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Clear symbol_state before backfill")
    args = parser.parse_args()
    
    conn = connect_db(DB_PATH)
    try:
        # Check existing rows
        cnt = conn.execute("SELECT COUNT(*) FROM symbol_state").fetchone()[0]
        if cnt > 0 and not args.force:
            print(f"[migrate] symbol_state already has {cnt} rows. Run with --force to rebuild.")
            return
        
        if args.force:
            print("[migrate] Force clearing symbol_state...")
            conn.execute("DELETE FROM symbol_state")
            
        print("[migrate] Beginning backfill process...")
        inserted = backfill_symbol_state(conn)
        print(f"[migrate] Backfilled symbol_state for {inserted} symbols.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
