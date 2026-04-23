import time
import datetime
import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from src.main import run_nightly_pipeline

def main():
    print("Scheduler started. Waiting for scheduled time (16:05 IST)...")
    while True:
        now = datetime.datetime.now()
        # Check if it's 4:05 PM (16:05)
        # Note: This assumes the system clock is in the correct timezone or handled appropriately.
        if now.hour == 16 and now.minute == 5:
            print(f"It's 16:05. Running pipeline...")
            run_nightly_pipeline()
            # Sleep for 60 seconds to avoid double execution in the same minute
            time.sleep(60)
        
        # Sleep for 30 seconds before checking again
        time.sleep(30)

if __name__ == "__main__":
    # For testing, you can run it immediately by calling run_nightly_pipeline() here
    # run_nightly_pipeline()
    main()
