import os
import datetime
import duckdb
import json
from config import connect_db

class ResearchJournal:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def add_entry(self, symbol, thesis, conviction, status, entry_trigger=None, risk_factors=None):
        conn = connect_db(self.db_path)
        try:
            # Get next ID
            next_id_res = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM research_journal").fetchone()
            next_id = next_id_res[0] if next_id_res else 1
            
            today = datetime.date.today()
            
            conn.execute("""
                INSERT INTO research_journal (id, symbol, date, thesis, conviction, status, entry_trigger, risk_factors)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (next_id, symbol, today, thesis, conviction, status, entry_trigger, risk_factors))
            
            print(f"Added journal entry for {symbol} with ID {next_id}")
            return next_id
        except Exception as e:
            print(f"Error adding journal entry: {e}")
            return None
        finally:
            conn.close()
            
    def upsert_state(
        self,
        symbol: str,
        action: str,
        conviction: int | None,
        thesis: str | None,
        entry_trigger: str | None,
        stop_loss: float | None = None,
        target: float | None = None,
        rejection_reason: str | None = None,
    ) -> None:
        conn = connect_db(self.db_path)
        try:
            # 1. Read existing state
            existing = conn.execute("SELECT * FROM symbol_state WHERE symbol = ?", (symbol,)).fetchone()
            
            # Extract descriptive names manually due to fetchone layout
            # Row layout expected from schema
            schema_cols = [
                'symbol', 'first_seen_date', 'last_seen_date', 'days_tracked', 'current_status',
                'current_conviction', 'current_thesis', 'current_entry_trigger', 'current_stop_loss', 'current_target',
                'conviction_history', 'status_history', 'trigger_history', 'trigger_static_days',
                'rejection_reason', 'last_action', 'last_run_date', 'updated_at'
            ]
            
            state_dict = {}
            if existing:
                state_dict = dict(zip(schema_cols, existing))
            
            today = datetime.date.today()
            
            # 2. Histories
            def parse_hist(s):
                try:
                    return json.loads(s) if s else []
                except:
                    return []
            
            conviction_history = parse_hist(state_dict.get('conviction_history'))
            status_history = parse_hist(state_dict.get('status_history'))
            trigger_history = parse_hist(state_dict.get('trigger_history'))
            
            # 3. Recompute trigger_static_days
            prior_trigger = state_dict.get('current_entry_trigger')
            prior_static_days = state_dict.get('trigger_static_days', 0) or 0
            prior_run_date = state_dict.get('last_run_date')
            
            trigger_static_days = 0
            if entry_trigger is not None and entry_trigger == prior_trigger:
                if prior_run_date:
                    delta = (today - prior_run_date).days
                    trigger_static_days = prior_static_days + max(delta, 1)
                else:
                    trigger_static_days = prior_static_days + 1
            
            # Update histories
            if conviction is not None:
                conviction_history.append({"date": str(today), "conviction": conviction})
            conviction_history = conviction_history[-10:]
            
            # Add trigger if changed
            if entry_trigger and entry_trigger != prior_trigger:
                trigger_history.append({"date": str(today), "entry_trigger": entry_trigger})
            trigger_history = trigger_history[-5:]
            
            # 4. Status mapping
            raw = action.upper() if action else ""
            
            # Check portfolio if needed
            is_open = False
            if raw in ["ENTER", "HOLD", "TRAIL_STOP"]:
                try:
                    p_res = conn.execute("SELECT COUNT(*) FROM portfolio WHERE symbol = ? AND status = 'OPEN'", (symbol,)).fetchone()
                    is_open = (p_res[0] > 0) if p_res else False
                except:
                    pass

            if raw == "ENTER":
                current_status = "ENTERED" if is_open else "TRIGGERED"
            elif raw in ["HOLD", "TRAIL_STOP"]:
                current_status = "ENTERED"
            elif raw == "EXIT":
                current_status = "EXITED"
            elif raw in ["WATCH_FOR_ENTRY", "WATCHLIST_ENTRY"]:
                current_status = "WATCHING"
            elif raw == "REMOVE_FROM_WATCHLIST":
                current_status = "REJECTED"
            else:
                current_status = state_dict.get('current_status') or "WATCHING"

            status_entry = {"date": str(today), "status": current_status}
            if rejection_reason:
                status_entry["reason"] = rejection_reason
            status_history.append(status_entry)
            status_history = status_history[-10:]

            # 5. Other computed fields
            first_seen = state_dict.get('first_seen_date') or today
            days_tracked = (today - first_seen).days
            
            # Clean thesis to handle truncation/newlines
            clean_thesis = thesis.strip() if thesis else None

            # 6. Upsert execution
            params = (
                symbol,
                first_seen,
                today, # last_seen
                days_tracked,
                current_status,
                conviction,
                clean_thesis,
                entry_trigger,
                stop_loss,
                target,
                json.dumps(conviction_history),
                json.dumps(status_history),
                json.dumps(trigger_history),
                trigger_static_days,
                rejection_reason,
                action, # original action case preserved
                today, # last_run
                datetime.datetime.now(),
                # ON CONFLICT updates:
                first_seen,
                today,
                days_tracked,
                current_status,
                conviction,
                clean_thesis,
                entry_trigger,
                stop_loss,
                target,
                json.dumps(conviction_history),
                json.dumps(status_history),
                json.dumps(trigger_history),
                trigger_static_days,
                rejection_reason,
                action,
                today,
                datetime.datetime.now()
            )
            
            conn.execute("""
                INSERT INTO symbol_state (
                    symbol, first_seen_date, last_seen_date, days_tracked, current_status,
                    current_conviction, current_thesis, current_entry_trigger, current_stop_loss, current_target,
                    conviction_history, status_history, trigger_history, trigger_static_days,
                    rejection_reason, last_action, last_run_date, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol) DO UPDATE SET
                    first_seen_date = ?,
                    last_seen_date = ?,
                    days_tracked = ?,
                    current_status = ?,
                    current_conviction = ?,
                    current_thesis = ?,
                    current_entry_trigger = ?,
                    current_stop_loss = ?,
                    current_target = ?,
                    conviction_history = ?,
                    status_history = ?,
                    trigger_history = ?,
                    trigger_static_days = ?,
                    rejection_reason = ?,
                    last_action = ?,
                    last_run_date = ?,
                    updated_at = ?
            """, params)
            print(f"Upserted symbol_state for {symbol} (status={current_status})")
            
        except Exception as e:
            print(f"Error upserting symbol state: {e}")
        finally:
            conn.close()

    def get_recent_notes(self, days=45):
        conn = connect_db(self.db_path)
        try:
            query = f"""
                SELECT * FROM research_journal 
                WHERE date > current_date - INTERVAL {days} DAY
                ORDER BY date DESC
            """
            return conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Error fetching notes: {e}")
            return None
        finally:
            conn.close()

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    base_dir = os.path.dirname(src_dir)
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    
    journal = ResearchJournal(db_path)
    journal.add_entry("RELIANCE", "Looks good for entry.", 8, "WATCHING", "Breakout above 2550")
    
    notes = journal.get_recent_notes()
    print(notes)
