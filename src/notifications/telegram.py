import os
import requests
from dotenv import load_dotenv

load_dotenv()

def send_telegram_message(message):
    """Sends a message to a Telegram chat, splitting it if it exceeds limits."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("Telegram credentials not found. Printing message instead:")
        print(message)
        return False
        
    MAX_LENGTH = 4096
    
    def send_chunk(text):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text
        }
        try:
            response = requests.post(url, json=payload)
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending Telegram chunk: {e}")
            return False

    if len(message) <= MAX_LENGTH:
        success = send_chunk(message)
        if success:
            print("Telegram message sent successfully.")
            return True
        else:
            print("Failed to send Telegram message.")
            return False
    else:
        print(f"Message too long ({len(message)} chars). Splitting into chunks...")
        chunks = [message[i:i+MAX_LENGTH] for i in range(0, len(message), MAX_LENGTH)]
        all_success = True
        for i, chunk in enumerate(chunks):
            print(f"Sending chunk {i+1}/{len(chunks)}...")
            if not send_chunk(chunk):
                all_success = False
                print(f"Failed to send chunk {i+1}")
        
        if all_success:
            print("All Telegram chunks sent successfully.")
            return True
        return False

if __name__ == "__main__":
    test_message = "*Test Message* from Positional Trading Bot.\n\nThis is a test."
    send_telegram_message(test_message)
