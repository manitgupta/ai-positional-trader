# Gemini-Powered Positional Trading Bot

A nightly batch system that behaves like a disciplined equity analyst for the Indian Stock Market (NSE/BSE). It scans the universe after market close, identifies positional trade setups (Minervini/O'Neil style), reviews open positions, and produces a structured research memo with actionable decisions using Gemini 2.5 Pro.

## Project Structure

```
trading-bot/
├── config.py                  # Configuration parameters
├── requirements.txt           # Python dependencies
├── scheduler.py               # Simple time check scheduler
├── data/
│   ├── schema.sql             # DuckDB schema
│   └── universe.duckdb        # Main database (created on init)
└── src/
    ├── pipeline/
    │   ├── initialize_db.py   # DB initialization script
    │   ├── load_universe.py   # Loads full NSE symbols list into DB
    │   ├── fetch_prices.py    # Price fetcher (Yahoo Finance)
    │   ├── compute_signals.py   # Signal computer (pandas-ta)
    │   ├── fetch_fundamentals.py # Fundamentals fetcher (Trendlyne Scraper)
    │   └── fetch_news.py        # News fetcher (Live RSS)
    ├── screener/
    │   ├── filters.py         # Hard-filter logic
    │   ├── scorer.py          # Composite scoring
    │   └── test_screener.py   # Screener test script
    ├── analyst/
    │   ├── prompts.py         # System prompt for Gemini
    │   ├── context_builder.py # Gemini context assembler
    │   ├── gemini_call.py     # API caller (google-genai)
    │   └── parser.py          # JSON extractor from memo
    ├── portfolio/
    │   ├── journal.py         # Research journal manager
    │   └── manager.py         # Portfolio state manager
    ├── notifications/
    │   └── telegram.py        # Telegram notification sender
    └── main.py                # Main orchestrator
```

## Setup Instructions

1.  **Environment**: Ensure you have a Python virtual environment set up and activated.
2.  **Dependencies**: Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Initialize Database**: Run the initialization script to create the DuckDB database and tables:
    ```bash
    python3 src/pipeline/initialize_db.py
    ```
4.  **Load Universe**: Populate the `universe` table with all NSE listed symbols:
    ```bash
    python3 src/pipeline/load_universe.py
    ```

## Configuration

Create a `.env` file in the root directory with the following variables:

```env
# Gemini API
GEMINI_API_KEY=your_gemini_api_key

# Telegram Notifications (Optional, prints to console if missing)
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## How to Run

### Test Run / Manual Run
To run the pipeline manually:
```bash
python3 src/main.py
```

### Running Scheduled
To run the scheduler that checks the time and executes the pipeline at 16:05 IST daily:
```bash
python3 scheduler.py
```

## Telegram Setup Instructions

To enable Telegram notifications:
1.  Search for `@BotFather` on Telegram.
2.  Send `/newbot` and follow instructions to get a **Bot Token**.
3.  Add the token to your `.env` file as `TELEGRAM_BOT_TOKEN`.
4.  To get your **Chat ID**, send a message to your bot and then visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`. Look for `"chat":{"id":...}` in the JSON response.
5.  Add the chat ID to your `.env` file as `TELEGRAM_CHAT_ID`.
