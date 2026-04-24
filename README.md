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

## The Stock Selection Process

Every night, the bot follows a disciplined multi-step process to identify high-conviction setups:

1. **Technical Hard Filters**: Scans the 2,300+ stock universe for strong Stage 2 uptrends, high Relative Strength (RS Rank), and price above the 200-day moving average.
2. **Multi-Timeframe Confirmation**: Pulls the last 5 weekly candles for top candidates to ensure the daily setup is supported by a strong weekly trend.
3. **Fundamental Verification**: Fetches data from Screener.in (EPS growth, Revenue growth, Promoter holding) to ensure the candidate is a healthy company.
4. **AI Analyst Review**: Passes the 30-day daily history, 5-week history, and fundamentals to **Gemini**. Gemini acts as a Minervini-style analyst, cross-verifying all data to produce a high-conviction thesis with specific entry triggers and stop losses.

## How to Use the Bot (Trading Strategy)

The bot is designed to be a nightly advisor. It does not execute trades automatically. Here is how you should use its output:

1. **Review the Daily Telegram Message**: Every day after market close, read the summary message sent to your Telegram.
2. **Focus on "Buy Setups"**: These are stocks in a low-risk entry position. 
   * **DO NOT buy them immediately** at the market open.
   * Set an alert in your trading terminal (e.g., Zerodha, Groww) for the specific **Entry Trigger** price provided by the bot.
   * Only take the trade if the stock crosses the trigger price on strong volume during market hours.
3. **Use the Watchlist**: These are great stocks that are currently too extended or need more time to consolidate. Add them to your broker's watchlist and wait for them to form a proper base. They may graduate to "Buy Setups" in future runs.
4. **Respect Risk Management**: Always set the **Stop Loss** provided by the bot to protect your capital.

