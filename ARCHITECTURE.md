# Architecture Documentation

This document details the technical architecture, data flow, and component structure of the Gemini-Powered Positional Trading Bot.

## Project Structure

```
trading-bot/
├── config.py                  # Configuration parameters
├── requirements.txt           # Python dependencies
├── scheduler.py               # Simple time check scheduler
├── data/
│   ├── schema.sql             # DuckDB schema
│   └── universe.duckdb        # Main database (tracked via Git LFS)
└── src/
    ├── pipeline/
    │   ├── initialize_db.py   # DB initialization script
    │   ├── load_universe.py   # Loads full NSE symbols list into DB
    │   ├── fetch_prices.py    # Price fetcher (Yahoo Finance, Incremental)
    │   ├── compute_signals.py   # Signal computer (pandas-ta, Incremental)
    │   ├── fetch_fundamentals.py # Fundamentals fetcher (Trendlyne Scraper, 7-day cache)
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

## Data Pipeline & Flow

The bot operates in a 7-phase nightly batch execution flow orchestrated by `src/main.py`:

### 1. Data Foundation (Prices & Signals)
*   **Prices**: Pulls daily EOD price history for the entire 2,336 stock universe from Yahoo Finance. It operates *incrementally*, only downloading dates missing from the local database.
*   **Signals**: Computes technical indicators (RSI, ADX, ATR, MACD, and 50/150/200 Moving Averages) using `pandas-ta`. It also operates incrementally to avoid re-processing the full history.
*   **RS Rank**: Computes a global Relative Strength percentile rank (0-100) for all stocks based on their 12-month raw momentum.

### 2. Screener (Technical Hard Filters)
*   Applies strict Minervini-style hard filters (e.g., RS Rank >= 70, Price > 200 MA, ADX > 20) to narrow down the universe to top momentum candidates.

### 3. Targeted Ingestion (Fundamentals & News)
*   **Fundamentals**: Scrapes quarterly metrics (EPS growth, RoE, Debt/Equity) from Trendlyne *only* for stocks that passed the technical filters. Results are cached for **7 days** to avoid rate limits.
*   **News**: Scrapes live RSS feeds from Moneycontrol and Economic Times for candidates.

### 4. Screener (Composite Scoring)
*   Ranks the final candidates using a composite score combining both technical momentum and fundamental metrics.

### 5. Gemini Analyst (Leg 1: Full Memo)
*   Passes the top candidates and their full context to **Gemini 2.5 Flash**.
*   Gemini acts as a SEPA analyst to generate full written research theses, risk factors, entry triggers, and stop losses.
*   The output includes structured JSON blocks.

### 6. Journal + Memory
*   Extracts the JSON decisions from the Gemini memo and commits them to the persistent local DuckDB store (`research_journal` and `portfolio` tables).

### 7. Gemini Analyst (Leg 2: Telegram Summary)
*   Calls Gemini a second time to condense the massive full report into a clean, visually rich summary utilizing emojis and code blocks, strictly fitting under Telegram's 4096 character limit.

### 8. Notifications
*   Delivers the rich summary directly to your configured Telegram chat.

## Persistence
*   **DuckDB**: All data sits in a highly efficient local DuckDB file at `data/universe.duckdb`.
*   **Git LFS**: Because the database grows to 60MB+, it is tracked in the repository using Git Large File Storage (LFS).
