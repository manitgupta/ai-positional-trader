import os
import datetime
import duckdb

class ResearchJournal:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def add_entry(self, symbol, thesis, conviction, status, entry_trigger=None, risk_factors=None):
        conn = duckdb.connect(self.db_path)
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
            
    def get_recent_notes(self, days=45):
        conn = duckdb.connect(self.db_path)
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
