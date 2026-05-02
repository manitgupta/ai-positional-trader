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

# Central list of tables to sync
KNOWN_TABLES = [
    'universe', 'prices', 'weekly_prices', 'signals', 'weekly_signals', 
    'annual_results', 'quarterly_results', 'news', 'research_journal', 
    'portfolio', 'account', 'delivery_data', 'symbol_state'
]

def get_md_connection():
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    if not md_token:
        print("Error: MOTHERDUCK_TOKEN not found in environment.")
        return None
    # Connect to MotherDuck (default)
    return duckdb.connect(f"md:?token={md_token}")

def push_to_motherduck():
    if not os.path.exists(LOCAL_DB_PATH):
        print(f"Local DB not found at {LOCAL_DB_PATH}")
        return
        
    conn = get_md_connection()
    if not conn:
        return
        
    print(f"Pushing data from {LOCAL_DB_PATH} to MotherDuck...")
    try:
        # Drop database if exists to start fresh with correct schema
        print(f"Recreating database {MD_DB_NAME} on MotherDuck...")
        conn.execute(f"DROP DATABASE IF EXISTS {MD_DB_NAME} CASCADE")
        conn.execute(f"CREATE DATABASE {MD_DB_NAME}")
        conn.execute(f"USE {MD_DB_NAME}")
        
        # Apply schema
        print("Applying schema to MotherDuck...")
        with open(os.path.join(base_dir, "data", "schema.sql"), "r") as f:
            schema_sql = f.read()
        conn.execute(schema_sql)
        
        # Attach local DB
        conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (TYPE DUCKDB)")
        
        for table in KNOWN_TABLES:
            print(f"Syncing table {table} to MotherDuck...")
            try:
                # Copy data into the table created by schema
                if table == 'account':
                    conn.execute(f"INSERT OR REPLACE INTO {MD_DB_NAME}.{table} SELECT * FROM local_db.{table}")
                else:
                    conn.execute(f"INSERT INTO {MD_DB_NAME}.{table} SELECT * FROM local_db.{table}")
                print(f"Success for {table}")
            except Exception as e:
                print(f"Error syncing {table}: {e}")
                
        # Ensure persistence
        conn.execute(f"CHECKPOINT {MD_DB_NAME}")
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
        
        # Attach local DB
        conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db (TYPE DUCKDB)")
        
        # Switch context to local_db so schema and drops run there automatically
        conn.execute("USE local_db")
        
        # Apply schema to local DB first
        print("Applying schema to local DB...")
        with open(os.path.join(base_dir, "data", "schema.sql"), "r") as f:
            schema_sql = f.read()
            
        # We need to drop tables in local DB first to enforce schema
        for table in KNOWN_TABLES:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                print(f"Error dropping local table {table}: {e}")
                
        # Now run schema directly on local DB context
        conn.execute(schema_sql)
        
        for table in KNOWN_TABLES:
            print(f"Syncing table {table} from MotherDuck...")
            try:
                # Copy data
                if table == 'account':
                    conn.execute(f"INSERT OR REPLACE INTO local_db.{table} SELECT * FROM {MD_DB_NAME}.{table}")
                else:
                    conn.execute(f"INSERT INTO local_db.{table} SELECT * FROM {MD_DB_NAME}.{table}")
                print(f"Success for {table}")
            except Exception as e:
                print(f"Error syncing {table}: {e}")
                
        # Ensure persistence to local disk
        conn.execute("CHECKPOINT local_db")
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
