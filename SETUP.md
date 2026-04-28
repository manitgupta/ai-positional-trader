# Setup and Execution Guide

This document guides you through setting up the environment, configuring credentials, and running the bot.

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

# MotherDuck (Optional for local, required for sync/CI)
MOTHERDUCK_TOKEN=your_motherduck_token
```


## Database Synchronization

The project uses a hybrid DuckDB and MotherDuck architecture. To sync data:

- **Push local data to MotherDuck**:
  ```bash
  python3 src/pipeline/sync_db.py push
  ```
- **Pull remote data from MotherDuck**:
  ```bash
  python3 src/pipeline/sync_db.py pull
  ```

## How to Run


### Test Run / Manual Run
To run the pipeline manually:
```bash
python3 src/main.py [--no-journal] [--no-telegram]
```

**Available Flags:**
- `--no-journal`: Skip saving entries to the research journal.
- `--no-telegram`: Skip generating summary and sending Telegram notifications.

Example:
```bash
python3 src/main.py --no-telegram
```

### Running Scheduled (Local)
To run the scheduler locally via the background daemon:
```bash
python3 scheduler.py
```

### Running Scheduled (GitHub Actions)
A GitHub Actions workflow is defined in `.github/workflows/nightly-run.yml`.
- It runs automatically at schedule specified in the workflow file (currently 12:37 UTC / 18:07 IST on weekdays).
- **Database Updates**: The workflow uses MotherDuck for data persistence. It does not commit the database file back to the repository.
- **Secrets**: Add your `GEMINI_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, and `MOTHERDUCK_TOKEN` as GitHub Secrets in your repository settings (`Settings -> Secrets and variables -> Actions`).


## Telegram Setup Instructions

To enable Telegram notifications:
1.  Search for `@BotFather` on Telegram.
2.  Send `/newbot` and follow instructions to get a **Bot Token**.
3.  Add the token to your `.env` file as `TELEGRAM_BOT_TOKEN`.
4.  To get your **Chat ID**, send a message to your bot and then visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`. Look for `"chat":{"id":...}` in the JSON response.
5.  Add the chat ID to your `.env` file as `TELEGRAM_CHAT_ID`.
