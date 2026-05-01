import os
import pandas as pd
import duckdb
from config import connect_db

def load_universe(csv_path, db_path):
    print(f"Loading universe from {csv_path} to {db_path}")
    
    # Skip the first 4 metadata lines
    df = pd.read_csv(csv_path, skiprows=4)
    
    # Strip whitespace from column names
    df.columns = [col.strip() for col in df.columns]
    
    # Select relevant columns and rename to match schema
    universe_df = df[['SYMBOL', 'NAME OF COMPANY', 'SERIES']].copy()
    universe_df.columns = ['symbol', 'company_name', 'series']
    
    # Strip whitespace from symbol
    universe_df['symbol'] = universe_df['symbol'].astype(str).str.strip()
    
    # Only include 'EQ' (Common Stock) and 'BE' series for main universe, exclude debt/mutual funds
    universe_df = universe_df[universe_df['series'].isin(['EQ', 'BE'])]
    
    print(f"Parsed {len(universe_df)} common stock equities.")
    
    conn = connect_db(db_path)
    try:
        conn.register('df_view', universe_df)
        conn.execute("""
            INSERT OR REPLACE INTO universe 
            SELECT symbol, company_name, series 
            FROM df_view
        """)
        print("Universe loaded into DuckDB successfully.")
    except Exception as e:
        print(f"Error loading universe into DB: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    csv_path = "/Users/manitgupta/.gemini/jetski/brain/5f09b57d-62fb-4cfe-bc07-bf76ce5aa849/.system_generated/steps/375/content.md"
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    base_dir = os.path.dirname(src_dir)
    db_path = os.path.join(base_dir, "data", "universe.duckdb")
    
    load_universe(csv_path, db_path)
