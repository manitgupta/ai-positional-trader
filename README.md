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

### Running Scheduled (Local)
To run the scheduler locally via the background daemon:
```bash
python3 scheduler.py
```

### Running Scheduled (GitHub Actions)
A GitHub Actions workflow is defined in `.github/workflows/nightly-run.yml`.
- It runs automatically at **16:05 IST (10:35 UTC)** on weekdays.
- **Database Updates**: Since GitHub Actions runs on ephemeral runners, the workflow automatically commits the updated `data/universe.duckdb` file and pushes it back to the repository's main branch at the end of each run, ensuring data persistence.
- **Secrets**: Add your `GEMINI_API_KEY`, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID` as GitHub Secrets in your repository settings (`Settings -> Secrets and variables -> Actions`).

## How to Use the Bot (Trading Strategy)

The bot is designed to be a nightly advisor. It does not execute trades automatically. Here is how you should use its output:

1. **Review the Nightly Telegram Message**: Every night after market close, read the summary message sent to your Telegram.
2. **Focus on "Buy Setups"**: These are stocks in a low-risk entry position. 
   * **DO NOT buy them immediately** at the market open.
   * Set an alert in your trading terminal (e.g., Zerodha, Groww) for the specific **Entry Trigger** price provided by the bot.
   * Only take the trade if the stock crosses the trigger price on strong volume during market hours.
3. **Use the Watchlist**: These are great stocks that are currently too extended or need more time to consolidate. Add them to your broker's watchlist and wait for them to form a proper base. They may graduate to "Buy Setups" in future runs.
4. **Respect Risk Management**: Always set the **Stop Loss** provided by the bot to protect your capital.

## Telegram Setup Instructions

To enable Telegram notifications:
1.  Search for `@BotFather` on Telegram.
2.  Send `/newbot` and follow instructions to get a **Bot Token**.
3.  Add the token to your `.env` file as `TELEGRAM_BOT_TOKEN`.
4.  To get your **Chat ID**, send a message to your bot and then visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`. Look for `"chat":{"id":...}` in the JSON response.
5.  Add the chat ID to your `.env` file as `TELEGRAM_CHAT_ID`.
