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
        # Add columns to signals table
        print("Checking/Adding columns to signals table...")
        
        # DuckDB doesn't have clean 'IF NOT EXISTS' for columns in ALTER TABLE usually
        # We can check if they exist first
        cols = conn.execute("PRAGMA table_info('signals')").fetchdf()
        existing_cols = cols['name'].tolist()
        
        if 'bb_width' not in existing_cols:
            print("Adding bb_width to signals...")
            conn.execute("ALTER TABLE signals ADD COLUMN bb_width DOUBLE;")
        else:
            print("bb_width already exists.")
            
        if 'daily_rs' not in existing_cols:
            print("Adding daily_rs to signals...")
            conn.execute("ALTER TABLE signals ADD COLUMN daily_rs DOUBLE;")
        else:
            print("daily_rs already exists.")
            
        # Create weekly_signals table
        print("Creating weekly_signals table if not exists...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weekly_signals (
                symbol          VARCHAR,
                date            DATE,
                sma_10          DOUBLE,
                sma_30          DOUBLE,
                rsi_14          DOUBLE,
                volume_ratio_10w DOUBLE,
                mansfield_rs    DOUBLE,
                PRIMARY KEY (symbol, date)
            );
        """)
        
        print("Migration completed successfully.")
        
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_migration()
