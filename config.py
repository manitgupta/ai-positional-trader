import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Universe
NSE_UNIVERSE_MIN_MCAP_CR = 500       # exclude micro caps below ₹500 Cr
NSE_UNIVERSE_MIN_AVG_VOLUME = 50000  # exclude illiquid stocks

# Screener hard filters
MIN_RS_RANK = 70
MIN_ADX = 20
MIN_EPS_GROWTH_YOY = 20              # %
MIN_REVENUE_GROWTH_YOY = 15          # %
MAX_PCT_FROM_52W_HIGH = -25          # %
MIN_VOLUME_RATIO_20D = 1.2

# Screener output
MAX_CANDIDATES_FOR_GEMINI = 25

# Portfolio
MAX_POSITIONS = 12
MAX_POSITION_SIZE_PCT = 10           # max 10% in any single stock
MAX_SECTOR_EXPOSURE_PCT = 25         # max 25% in any one sector
RISK_PER_TRADE_PCT = 2               # max 2% portfolio at risk per trade

# Gemini
GEMINI_MODEL = "gemini-3.1-pro-preview"

# Schedule
RUN_TIME_IST = "16:05"               # 35 min after market close

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = "md:trading_db" if os.environ.get("USE_MOTHERDUCK") == "true" else os.path.join(BASE_DIR, "data", "universe.duckdb")
SCHEMA_PATH = os.path.join(BASE_DIR, "data", "schema.sql")
