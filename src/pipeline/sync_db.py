import os
import sys
import duckdb
from dotenv import load_dotenv

load_dotenv(override=True)

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
base_dir = os.path.dirname(src_dir)
sys.path.append(base_dir)

from config import DB_PATH # This points to local file or MD string depending on env

# For sync, we need the SPECIFIC paths
LOCAL_DB_PATH = os.path.join(base_dir, "data", "universe.duckdb")
MD_DB_NAME = "trading_db"

def get_md_connection():
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    if not md_token:
        print("Error: MOTHERDUCK_TOKEN not found in environment.")
        return None
    # Connect to MotherDuck
    return duckdb.connect(f"md:{MD_DB_NAME}?token={md_token}")

def push_to_motherduck():
    if not os.path.exists(LOCAL_DB_PATH):
        print(f"Local DB not found at {LOCAL_DB_PATH}")
        return
        
    conn = get_md_connection()
    if not conn:
        return
        
    print(f"Pushing data from {LOCAL_DB_PATH} to MotherDuck...")
    try:
        # Attach local DB
        conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (TYPE DUCKDB)")
        
        known_tables = ['universe', 'prices', 'weekly_prices', 'signals', 'annual_results', 'quarterly_results', 'news', 'research_journal', 'portfolio', 'account']
        
        for table in known_tables:
            print(f"Syncing table {table} to MotherDuck...")
            try:
                # Overwrite table on MotherDuck with local data
                conn.execute(f"CREATE OR REPLACE TABLE {MD_DB_NAME}.{table} AS SELECT * FROM local_db.{table}")
                print(f"Success for {table}")
            except Exception as e:
                print(f"Error syncing {table}: {e}")
                
        print("Push completed successfully!")
    except Exception as e:
        print(f"Error during push: {e}")
    finally:
        conn.close()

def pull_from_motherduck():
    conn = get_md_connection()
    if not conn:
        return
        
    print(f"Pulling data from MotherDuck to {LOCAL_DB_PATH}...")
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)
        
        # Attach local DB (creates file if not exists)
        conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (TYPE DUCKDB)")
        
        known_tables = ['universe', 'prices', 'weekly_prices', 'signals', 'annual_results', 'quarterly_results', 'news', 'research_journal', 'portfolio', 'account']
        
        for table in known_tables:
            print(f"Syncing table {table} from MotherDuck...")
            try:
                # Overwrite local table with MotherDuck data
                conn.execute(f"CREATE OR REPLACE TABLE local_db.{table} AS SELECT * FROM {MD_DB_NAME}.{table}")
                print(f"Success for {table}")
            except Exception as e:
                print(f"Error syncing {table}: {e}")
                
        print("Pull completed successfully!")
    except Exception as e:
        print(f"Error during pull: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sync_db.py [push|pull]")
        sys.exit(1)
        
    action = sys.argv[1].lower()
    if action == "push":
        push_to_motherduck()
    elif action == "pull":
        pull_from_motherduck()
    else:
        print(f"Unknown action: {action}. Use 'push' or 'pull'.")
        sys.exit(1)
