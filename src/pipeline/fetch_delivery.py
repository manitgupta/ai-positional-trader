import os
import datetime
import pandas as pd
import duckdb
from pathlib import Path
from nse import NSE
import time

class DeliveryFetcher:
    def __init__(self, db_path):
        self.db_path = db_path
        self.download_dir = Path(os.path.dirname(db_path)) / "downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
    def get_last_updated_date(self):
        """Get the latest date stored in the delivery_data table."""
        conn = duckdb.connect(self.db_path)
        try:
            res = conn.execute("SELECT MAX(date) FROM delivery_data").fetchone()
            if res and res[0]:
                if isinstance(res[0], str):
                    return datetime.datetime.strptime(res[0], "%Y-%m-%d").date()
                return res[0]
            return None
        except Exception:
            return None
        finally:
            conn.close()

    def process_bhavcopy_file(self, file_path, target_date):
        """Read NSE CSV file, clean columns, convert types and return clean DataFrame."""
        try:
            # Read CSV. Be lenient on encoding.
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Clean column whitespace
            df.columns = [c.strip() for c in df.columns]
            
            # Filter for standard Equities series
            if 'SERIES' in df.columns:
                df = df[df['SERIES'].str.strip() == 'EQ'].copy()
            
            needed_cols = ['SYMBOL', 'TTL_TRD_QNTY', 'DELIV_QTY', 'DELIV_PER']
            for col in needed_cols:
                if col not in df.columns:
                    print(f"Warning: Missing column {col} in {file_path}")
                    return pd.DataFrame()
            
            final_df = pd.DataFrame()
            final_df['symbol'] = df['SYMBOL'].str.strip()
            final_df['date'] = target_date
            
            # Convert numeric columns, handling '-' or invalid characters
            final_df['traded_qty'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
            final_df['deliverable_qty'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
            final_df['delivery_pct'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
            
            # Drop rows without critical data
            final_df = final_df.dropna(subset=['traded_qty', 'deliverable_qty'])
            
            return final_df
        except Exception as e:
            print(f"Error parsing file {file_path}: {e}")
            return pd.DataFrame()

    def save_to_db(self, df):
        """Upsert DataFrame to DuckDB."""
        if df.empty:
            return
            
        conn = duckdb.connect(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT INTO delivery_data 
                SELECT symbol, date, traded_qty, deliverable_qty, delivery_pct
                FROM df_view
                ON CONFLICT(symbol, date) DO UPDATE SET 
                    traded_qty = excluded.traded_qty,
                    deliverable_qty = excluded.deliverable_qty,
                    delivery_pct = excluded.delivery_pct
            """)
            print(f"Successfully ingested {len(df)} delivery records for {df['date'].iloc[0]}")
        except Exception as e:
            print(f"Error saving to database: {e}")
        finally:
            conn.close()

    def fetch_for_date(self, target_date):
        """Fetch and save delivery data for one specific date."""
        print(f"Attempting to fetch delivery bhavcopy for {target_date}...")
        
        # Ensure it is a datetime object for the NSE library
        if isinstance(target_date, datetime.date) and not isinstance(target_date, datetime.datetime):
            dt = datetime.datetime.combine(target_date, datetime.datetime.min.time())
        else:
            dt = target_date
            
        try:
            with NSE(download_folder=str(self.download_dir)) as nse_client:
                # Call library
                file_path = nse_client.deliveryBhavcopy(date=dt)
                
                if not file_path or not Path(file_path).exists():
                    print(f"No file returned by NSE for {target_date} (could be a holiday/weekend).")
                    return False
                
                # Process file
                clean_df = self.process_bhavcopy_file(file_path, target_date)
                
                if not clean_df.empty:
                    self.save_to_db(clean_df)
                    # Cleanup temp download after ingestion
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                    return True
                else:
                    print(f"File was empty or parsing failed for {target_date}")
                    return False
        except Exception as e:
            # The library might throw 404 errors for holidays/weekends - log but continue.
            err_str = str(e)
            if "404" in err_str:
                 print(f"No data available for {target_date} (NSE returned 404, likely holiday).")
            else:
                 print(f"Exception fetching date {target_date}: {e}")
            return False

    def fetch_latest(self):
        """Utility to fetch the most recent published delivery data automatically."""
        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo("Asia/Kolkata")
        except ImportError:
            # Fallback if zoneinfo missing on old versions, though present in env
            tz = None
            
        now_local = datetime.datetime.now(tz)
        today = now_local.date()
        
        # NSE typically uploads composite bhavcopy files by 6-7 PM, using 8 PM to be maximally safe
        if now_local.time() >= datetime.time(20, 0):
            target_date = today
        else:
            target_date = today - datetime.timedelta(days=1)
            
        # Check if target is weekend (Saturday=5, Sunday=6) - roll back to Friday
        if target_date.weekday() == 5: # Sat
            target_date = target_date - datetime.timedelta(days=1)
        elif target_date.weekday() == 6: # Sun
            target_date = target_date - datetime.timedelta(days=2)
            
        # Check if already in DB
        conn = duckdb.connect(self.db_path)
        exists = conn.execute("SELECT COUNT(*) FROM delivery_data WHERE date = ?", (target_date,)).fetchone()[0]
        conn.close()
        
        if exists > 0:
            print(f"Delivery data for {target_date} is already populated in DB.")
            return
            
        self.fetch_for_date(target_date)

if __name__ == "__main__":
    # Example local trigger
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_file = os.path.join(os.path.dirname(os.path.dirname(current_dir)), "data", "universe.duckdb")
    
    fetcher = DeliveryFetcher(db_file)
    fetcher.fetch_latest()
