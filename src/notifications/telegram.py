import os
import requests
from dotenv import load_dotenv

load_dotenv()

def send_telegram_message(message):
    """Sends a message to a Telegram chat, splitting it by lines if it exceeds limits."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("Telegram credentials not found. Printing message instead:")
        print(message)
        return False
        
    MAX_LENGTH = 4000 # Leave buffer for safety
    
    def send_chunk(text, parse_mode="Markdown"):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
            
        try:
            response = requests.post(url, json=payload)
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending Telegram chunk: {e}")
            return False

    if len(message) <= MAX_LENGTH:
        if send_chunk(message, "Markdown"):
            print("Telegram message sent successfully.")
            return True
        else:
            print("Failed with Markdown. Retrying as plain text...")
            return send_chunk(message, None)
            
    print(f"Message too long ({len(message)} chars). Splitting by lines...")
    lines = message.split('\n')
    chunks = []
    current_chunk = ""
    
    for line in lines:
        if len(current_chunk) + len(line) + 1 <= MAX_LENGTH:
            current_chunk += line + '\n'
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = line + '\n'
            
    if current_chunk:
        chunks.append(current_chunk.strip())
        
    all_success = True
    for i, chunk in enumerate(chunks):
        print(f"Sending chunk {i+1}/{len(chunks)}...")
        if not send_chunk(chunk, "Markdown"):
            print(f"Markdown failed for chunk {i+1}. Retrying as plain text...")
            if send_chunk(chunk, None):
                print(f"Chunk {i+1} sent as plain text.")
            else:
                print(f"Failed to send chunk {i+1} even as plain text.")
                all_success = False
                
    return all_success

if __name__ == "__main__":
    test_message = "*Test Message* from Positional Trading Bot.\n\nThis is a test."
    send_telegram_message(test_message)
