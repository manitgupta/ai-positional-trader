# Gemini-Powered Positional Trading Bot

A nightly batch system that behaves like a disciplined equity analyst for the Indian Stock Market (NSE/BSE). It scans the universe after market close, identifies positional trade setups (Minervini/O'Neil style), reviews open positions, and produces a structured research memo with actionable decisions using Gemini.

## Project Structure

For detailed technical architecture, data flow pipeline, and folder structure, please see the [ARCHITECTURE.md](file:///Users/manitgupta/experiments/ai-positional-trader/ARCHITECTURE.md) file.

## Setup & Execution

For detailed instructions on how to:
- Set up the environment and database
- Configure credentials and Telegram
- Run the bot manually or scheduled

Please refer to the [SETUP.md](file:///Users/manitgupta/experiments/ai-positional-trader/SETUP.md) file.

## The Stock Selection Process (Methodology)

Every night, the bot follows a disciplined multi-phase process to identify high-conviction setups, moving from brute-force data filtering to deep AI analysis.

### Phase 1: Data Foundation
- **Universe**: The bot scans a universe of ~2,300 common stocks (Series 'EQ').
- **Daily Prices**: Fetches daily OHLCV data to compute technical indicators.
- **Weekly Prices**: Fetches weekly data to confirm long-term Stage-2 uptrends.

### Phase 2: Screener (SQL Filters & Ranking)
Instead of rigid hard filters that might miss early-stage setups (like cup-and-handle bases or power plays), the bot uses loose "data-hygiene" gates followed by a composite scoring system to shortlist the best names:
1. **Liquidity Gate**: Ensures stock is tradable (Price >= ₹50, 50-day average turnover >= ₹10 crore, Series='EQ').
2. **Trend Liveness Gate**: Catches emergent Stage-2 names (Price within 50% of 52-w high OR making a higher high vs 3 months ago OR 50-DMA > 200-DMA).
3. **Composite Scoring**: Surviving candidates (~800 names) are ranked by a score computed in DuckDB SQL and Pandas:
   * **Weighted RS (35%)**: IBD-style weighted returns (40% to 3m, 20% each to 6m, 9m, 12m).
   * **Proximity to High (25%)**: Favors stocks trading near 52-week highs.
   * **Base Tightness (25%)**: Favors low-volatility consolidations (inverse of ATR/Close).
   * **Sector RS (15%)**: Favors stocks in leading sectors.

The top **30 candidates** from this ranking are passed to the AI Analyst.

### Phase 3: Data Enrichment
- Fetches fresh **Quarterly Fundamentals** and **News** from Screener.in for the top 30 candidates only, saving time and avoiding rate limits.

### Phase 4: AI Analyst (LangGraph Flow)
- The bot uses a **LangGraph** workflow to analyze candidates in parallel.
- **Candidate Evaluators**: Each candidate is evaluated by a dedicated Gemini call using tools to fetch detailed daily charts, weekly charts, quarterly results, and news *on demand*.
- **Synthesizer Agent**: Combines the individual evaluations into a single nightly research memo.
- **Output**: Produces a nightly research memo in three sections:
  * **SECTION 1: PORTFOLIO REVIEW**: Reviews open positions and manages risk.
  * **SECTION 2: NEW OPPORTUNITIES**: Full investment thesis for top setups (Conviction >= 7) with detailed evidence.
  * **SECTION 3: WATCHLIST**: Stocks to track for specific triggers.

### Code 33 Earnings Acceleration
The system detects Mark Minervini's "Code 33" pattern (3 consecutive quarters of accelerating YoY growth in EPS and Sales) in the database and exposes this flag to Gemini to help it identify elite fundamental momentum.


## How to Use the Bot (Trading Strategy)

The bot is designed to be a nightly advisor. It does not execute trades automatically. Here is how you should use its output:

1. **Review the Daily Telegram Message**: Every day after market close, read the summary message sent to your Telegram.
2. **Focus on "Buy Setups"**: These are stocks in a low-risk entry position. 
   * **DO NOT buy them immediately** at the market open.
   * Set an alert in your trading terminal (e.g., Zerodha, Groww) for the specific **Entry Trigger** price provided by the bot.
   * Only take the trade if the stock crosses the trigger price on strong volume during market hours.
3. **Use the Watchlist**: These are great stocks that are currently too extended or need more time to consolidate. Add them to your broker's watchlist and wait for them to form a proper base. They may graduate to "Buy Setups" in future runs.
4. **Respect Risk Management**: Always set the **Stop Loss** provided by the bot to protect your capital.

