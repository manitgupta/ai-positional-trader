import os
import requests
import html
import re
from dotenv import load_dotenv

load_dotenv()

def sanitize_telegram_html(html_content):
    """Sanitizes HTML content to only include tags supported by Telegram."""
    # Replace common unsupported tags with supported ones or text equivalents
    html_content = re.sub(r'<h1>(.*?)</h1>', r'<b>\1</b>\n', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'<h2>(.*?)</h2>', r'<b>\1</b>\n', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'<h3>(.*?)</h3>', r'<b>\1</b>\n', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'<p>(.*?)</p>', r'\1\n', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'<li>(.*?)</li>', r'• \1\n', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'<ul>(.*?)</ul>', r'\1', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<ol>(.*?)</ol>', r'\1', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Strip any other remaining tags that Telegram doesn't support
    html_content = re.sub(r'<(?!\/?(b|strong|i|em|u|ins|s|strike|del|a|code|pre)\b)[^>]+>', '', html_content)
    
    return html_content

def send_telegram_message(message):
    """Sends a message to a Telegram chat, converting markdown to HTML and splitting if needed."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("Telegram credentials not found. Printing message instead:")
        print(message)
        return False
        
    # Convert to HTML (Gemini outputs HTML now, so we just sanitize it)
    html_message = sanitize_telegram_html(message)
    
    MAX_LENGTH = 4000 # Leave buffer
    
    def send_chunk(text, parse_mode="HTML"):
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
            
        try:
            response = requests.post(url, json=payload)
            if response.status_code != 200:
                print(f"Telegram API error: {response.status_code} - {response.text}")
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending Telegram chunk: {e}")
            return False

    if len(html_message) <= MAX_LENGTH:
        if send_chunk(html_message):
            print("Telegram message sent successfully.")
            return True
        else:
            print("Failed with HTML. Retrying as plain text...")
            return send_chunk(message, None)
            
    print(f"Message too long ({len(html_message)} chars). Splitting by lines...")
    lines = html_message.split('\n')
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
        if not send_chunk(chunk):
            print(f"HTML failed for chunk {i+1}. Retrying as plain text...")
            # Strip HTML tags for fallback
            plain_chunk = re.sub(r'<[^>]+>', '', chunk)
            plain_chunk = html.unescape(plain_chunk)
            
            if send_chunk(plain_chunk, None):
                print(f"Chunk {i+1} sent as plain text.")
            else:
                print(f"Failed to send chunk {i+1} even as plain text.")
                all_success = False
                
    return all_success

def send_telegram_document(file_path, caption=None):
    """Sends a document to a Telegram chat."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("Telegram credentials not found. Cannot send document.")
        return False
        
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    
    try:
        with open(file_path, 'rb') as f:
            files = {'document': f}
            data = {'chat_id': chat_id}
            if caption:
                data['caption'] = caption
                
            response = requests.post(url, data=data, files=files)
            
            if response.status_code == 200:
                print("Document sent successfully.")
                return True
            else:
                print(f"Failed to send document: {response.status_code} - {response.text}")
                return False
    except Exception as e:
        print(f"Error sending document: {e}")
        return False

if __name__ == "__main__":
    test_message = "*Test Message* from Positional Trading Bot.\n\nThis is a test."
    send_telegram_message(test_message)
    
    # Test send_telegram_document
    test_file = "test_memo.txt"
    with open(test_file, "w") as f:
        f.write("This is a test memo for Telegram document attachment.")
    
    print("Testing send_telegram_document...")
    send_telegram_document(test_file, caption="Test Memo File")
    
    import os
    if os.path.exists(test_file):
        os.remove(test_file)
