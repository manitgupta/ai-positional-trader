import os
import datetime
import pandas as pd
import duckdb
from config import connect_db
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

class NewsFetcher:
    def __init__(self, db_path):
        self.db_path = db_path
        # Common RSS feeds for Indian markets
        self.feeds = {
            'moneycontrol': 'https://www.moneycontrol.com/rss/latestnews.xml',
            'et_markets': 'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms'
        }
        if not os.environ.get("GEMINI_API_KEY"):
            raise ValueError("GEMINI_API_KEY environment variable is required.")
        self.client = genai.Client()
        
    def fetch_news_for_symbol(self, symbol):
        """Fetch news headlines from RSS feeds and filter for symbol."""
        print(f"Fetching real news for {symbol}...")
        all_items = []
        
        # Get company name for better matching
        with connect_db(self.db_path) as conn:
            row = conn.execute("SELECT company_name FROM universe WHERE symbol = ?", (symbol,)).fetchone()
            company_name = row[0] if row else symbol
            
        print(f"Matching for symbol: {symbol} or Company: {company_name}")
        
        for source, url in self.feeds.items():
            try:
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                if response.status_code == 200:
                    root = ET.fromstring(response.content)
                    for item in root.findall('.//item'):
                        title = item.find('title').text if item.find('title') is not None else ""
                        desc = item.find('description').text if item.find('description') is not None else ""
                        
                        # Check if symbol or company name is mentioned
                        match = False
                        if symbol.lower() in title.lower() or company_name.lower() in title.lower():
                            match = True
                        elif symbol.lower() in desc.lower() or company_name.lower() in desc.lower():
                            match = True
                            
                        if match:
                            all_items.append({
                                'title': title,
                                'description': desc,
                                'source': source,
                                'pubDate': item.find('pubDate').text if item.find('pubDate') is not None else ""
                            })
            except Exception as e:
                print(f"Error fetching from {source}: {e}")
                
        return all_items

    def analyze_sentiment(self, text):
        """Analyze sentiment using Gemini Flash."""
        prompt = f"""
        Analyze the sentiment of the following financial news item.
        Return a JSON object with the following keys:
        - score: float between -1.0 (very negative) and 1.0 (very positive).
        - material: boolean indicating if this is a material event for the company.
        - summary: a short 1-sentence summary of the impact.

        News: {text}
        """
        try:
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1
                )
            )
            import json
            result = json.loads(response.text)
            return result.get('score', 0.0), result.get('material', False), result.get('summary', '')
        except Exception as e:
            print(f"Error analyzing sentiment with Gemini: {e}")
            return 0.0, False, "Error analyzing sentiment"

    def process_news(self, symbol):
        items = self.fetch_news_for_symbol(symbol)
        
        if not items:
            print(f"No specific news found for {symbol} in current feeds.")
            return pd.DataFrame()
            
        # Take the latest
        latest = items[0]
        text = latest['title'] + " " + latest['description']
        score, material, summary = self.analyze_sentiment(text)
        
        data = {
            'symbol': [symbol],
            'date': [datetime.date.today()],
            'sentiment_score': [score],
            'material_event': [material],
            'summary': [summary if summary else latest['title']]
        }
        return pd.DataFrame(data)
        
    def save_to_db(self, df):
        if df.empty:
            return
            
        conn = connect_db(self.db_path)
        try:
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR REPLACE INTO news 
                SELECT symbol, date, sentiment_score, material_event, summary 
                FROM df_view
            """)
            print(f"Saved news for {df['symbol'].iloc[0]} to DB.")
        except Exception as e:
            print(f"Error saving news: {e}")
        finally:
            conn.close()

