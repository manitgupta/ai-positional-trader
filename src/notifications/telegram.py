import os
import requests
from dotenv import load_dotenv

load_dotenv()

def send_telegram_message(message):
    """
    Sends a message to a Telegram chat.
    
    Instructions to set up Telegram Bot:
    1. Search for '@BotFather' on Telegram.
    2. Send '/newbot' and follow instructions to get a Bot Token.
    3. Add the token to your .env file as TELEGRAM_BOT_TOKEN.
    4. To get your Chat ID, send a message to your bot and then visit:
       https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
       Look for "chat":{"id":...} in the JSON response.
    5. Add the chat ID to your .env file as TELEGRAM_CHAT_ID.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("Telegram credentials not found. Printing message instead:")
        print(message)
        return False
        
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Telegram message sent successfully.")
            return True
        else:
            print(f"Failed to send Telegram message. Status: {response.status_code}, Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error sending Telegram message: {e}")
        return False

if __name__ == "__main__":
    test_message = "*Test Message* from Positional Trading Bot.\n\nThis is a test."
    send_telegram_message(test_message)
