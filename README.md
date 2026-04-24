# Gemini-Powered Positional Trading Bot

A nightly batch system that behaves like a disciplined equity analyst for the Indian Stock Market (NSE/BSE). It scans the universe after market close, identifies positional trade setups (Minervini/O'Neil style), reviews open positions, and produces a structured research memo with actionable decisions using Gemini.

## Project Structure

For detailed technical architecture, data flow pipeline, and folder structure, please see the [ARCHITECTURE.md](file:///Users/manitgupta/experiments/ai-positional-trader/ARCHITECTURE.md) file.

## Setup & Usage

For detailed instructions on:
- Setting up the environment and database
- Configuration via `.env`
- Running the bot manually or scheduled

Please refer to the [ARCHITECTURE.md](file:///Users/manitgupta/experiments/ai-positional-trader/ARCHITECTURE.md) file.

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
