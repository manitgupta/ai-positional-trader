import duckdb
import os
import sys

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import DB_PATH

def run_migration():
    print(f"Connecting to database at {DB_PATH}")
    conn = duckdb.connect(DB_PATH)
    try:
        print("Checking/Adding columns to quarterly_results table...")
        cols = conn.execute("PRAGMA table_info('quarterly_results')").fetchdf()
        existing_cols = cols['name'].tolist()
        
        if 'promoter_holding' not in existing_cols:
            print("Adding promoter_holding to quarterly_results...")
            conn.execute("ALTER TABLE quarterly_results ADD COLUMN promoter_holding DOUBLE;")
        else:
            print("promoter_holding already exists.")
            
        if 'fii_holding' not in existing_cols:
            print("Adding fii_holding to quarterly_results...")
            conn.execute("ALTER TABLE quarterly_results ADD COLUMN fii_holding DOUBLE;")
        else:
            print("fii_holding already exists.")
            
        if 'dii_holding' not in existing_cols:
            print("Adding dii_holding to quarterly_results...")
            conn.execute("ALTER TABLE quarterly_results ADD COLUMN dii_holding DOUBLE;")
        else:
            print("dii_holding already exists.")
            
        print("Migration completed successfully.")
        
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_migration()
