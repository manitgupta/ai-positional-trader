import duckdb
import os
from config import connect_db

def initialize_db(db_path, schema_path):
    print(f"Initializing database at {db_path} using schema {schema_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to DuckDB (creates file if not exists)
    conn = connect_db(db_path)
    
    # Read schema
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
        
    # Execute schema
    try:
        conn.execute(schema_sql)
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Calculate paths relative to this script's location
    # This script is in src/pipeline/
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    base_dir = os.path.dirname(src_dir)
    
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    schema_path = os.path.join(base_dir, "data", "schema.sql")
    
    initialize_db(db_path, schema_path)
