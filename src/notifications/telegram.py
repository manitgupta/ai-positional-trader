import os
import json
import datetime
from markdown_pdf import MarkdownPdf, Section
from src.analyst.gemini_call import GeminiAnalyst
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

def json_to_markdown_table(json_str):
    """Converts a JSON string containing a 'decisions' array into a markdown table."""
    try:
        data = json.loads(json_str)
        if not isinstance(data, dict) or 'decisions' not in data:
            return "```json\n" + json_str + "\n```" # Return as is if not expected format
            
        decisions = data['decisions']
        if not decisions:
            return "*No decisions in this section.*"
            
        # Get all unique keys from all objects to form headers
        headers = set()
        for d in decisions:
            headers.update(d.keys())
        
        # We want a specific order if possible, otherwise alphabetical
        preferred_order = ['symbol', 'ticker', 'action', 'conviction', 'entry_trigger', 'entry_zone', 'stop_loss', 'target', 'position_size_pct', 'thesis', 'justification']
        sorted_headers = []
        for h in preferred_order:
            if h in headers:
                sorted_headers.append(h)
                headers.remove(h)
        sorted_headers.extend(sorted(list(headers))) # Add remaining keys alphabetically
        
        # Build table header
        table = "| " + " | ".join([h.replace('_', ' ').title() for h in sorted_headers]) + " |\n"
        table += "| " + " | ".join(["---"] * len(sorted_headers)) + " |\n"
        
        # Build rows
        for d in decisions:
            row = []
            for h in sorted_headers:
                val = d.get(h, "")
                if isinstance(val, list):
                    val = ", ".join(map(str, val))
                elif val is None:
                    val = ""
                row.append(str(val).replace('\n', ' ')) # Replace newlines with spaces to avoid breaking the table
            table += "| " + " | ".join(row) + " |\n"
            
        return table
    except json.JSONDecodeError:
        return "```json\n" + json_str + "\n```" # Return as is if invalid JSON
    except Exception as e:
        print(f"Error converting JSON to table: {e}")
        return "```json\n" + json_str + "\n```"

def preprocess_memo(text):
    """Finds JSON blocks in the text and converts them to markdown tables."""
    pattern = r'```json\s*([\s\S]*?)\s*```'
    
    def replacer(match):
        json_str = match.group(1).strip()
        return json_to_markdown_table(json_str)
        
    return re.sub(pattern, replacer, text)

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

def send_research_report(memo, no_telegram=False):
    """Generates a summary and sends it to Telegram along with the full memo as a PDF."""
    if no_telegram:
        print("\nSkipping Telegram summary and notifications due to --no-telegram flag.")
        return True
        
    print("\n--- Generating Telegram Summary ---")
    today = datetime.date.today().strftime("%B %d, %Y")
    summary_prompt = f"""
    You are a professional equity research editor. Summarize the research memo above into a visually stunning, highly readable Telegram message using HTML tags.
    
    Current Date: {today}
    
    Follow these styling rules to make it look rich and premium:
    1. Use Emojis extensively to add color and structure (e.g., 🚀 for Buy Setups, 👀 for Watchlist, 🎯 for Targets, 🛑 for Stop Loss, 📈 for RS Rank).
    2. Use <b>ALL CAPS BOLD</b> for section headers.
    3. Use <pre>...</pre> to display key metrics and triggers cleanly.
    4. Keep it under 4000 characters so it fits in a single message.
    5. Output ONLY valid HTML. Do NOT use Markdown tags like ** or *.
    
    Telegram supports only these tags: <b>, <i>, <u>, <s>, <a>, <code>, <pre>. Do NOT use any other tags like <p>, <h1>, <ul> etc.
    
    Structure the message with:
    - A professional header with the date {today}.
    - A 📊 <b>PORTFOLIO REVIEW</b> section summarizing the status of open positions and any actions needed.
    - A 🚀 <b>BUY SETUPS</b> section with clean, structured details and detailed evidence of why it passed for each top candidate.
    - A 👀 <b>WATCHLIST</b> section split into:
      ✨ <i>New Today</i> — fresh from today's screen
      📌 <i>Carried</i> — note days_tracked and conviction trajectory
      ❌ <i>Demoted</i> — with reason
    """
    
    try:
        summary = GeminiAnalyst().generate_summary(memo, summary_prompt)
        
        print("\n--- Sending Notifications ---")
        send_telegram_message(summary)
        
        # Generate and send PDF
        file_name = f"Research_Memo_{today.replace(' ', '_').replace(',', '')}.pdf"
        file_path = os.path.join(os.getcwd(), file_name)
        
        print(f"Generating PDF: {file_path}...")
        pdf = MarkdownPdf()
        pdf.add_section(Section(preprocess_memo(memo), toc=False))
        pdf.save(file_path)
        
        print(f"Sending PDF file...")
        send_telegram_document(file_path, caption=f"Full Research Memo - {today}")
        
        # Clean up
        os.remove(file_path)
        return True
    except Exception as e:
        print(f"Error in send_research_report: {e}")
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        return False

if __name__ == "__main__":
    test_message = "*Test Message* from Positional Trading Bot.\n\nThis is a test."
    send_telegram_message(test_message)
