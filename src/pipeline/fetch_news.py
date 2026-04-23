import os
import datetime
import pandas as pd
import duckdb
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

class NewsFetcher:
    def __init__(self, db_path):
        self.db_path = db_path
        # Common RSS feeds for Indian markets
        self.feeds = {
            'moneycontrol': 'https://www.moneycontrol.com/rss/latestnews.xml',
            'et_markets': 'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms'
        }
        
    def fetch_news_for_symbol(self, symbol):
        """Fetch news headlines from RSS feeds and filter for symbol."""
        print(f"Fetching real news for {symbol}...")
        all_items = []
        
        for source, url in self.feeds.items():
            try:
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                if response.status_code == 200:
                    root = ET.fromstring(response.content)
                    for item in root.findall('.//item'):
                        title = item.find('title').text if item.find('title') is not None else ""
                        desc = item.find('description').text if item.find('description') is not None else ""
                        
                        # Simple check if symbol is mentioned in title or description
                        if symbol.lower() in title.lower() or symbol.lower() in desc.lower():
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
        """Lightweight rule-based sentiment scoring."""
        positive_words = ['grow', 'profit', 'beat', 'surge', 'gain', 'rise', 'positive', 'buy', 'bullish']
        negative_words = ['drop', 'loss', 'miss', 'fall', 'decline', 'negative', 'sell', 'bearish', 'cut']
        
        score = 0.0
        words = text.lower().split()
        
        for word in words:
            if word in positive_words:
                score += 0.2
            elif word in negative_words:
                score -= 0.2
                
        # Clamp score between -1 and 1
        return max(-1.0, min(1.0, score))

    def process_news(self, symbol):
        items = self.fetch_news_for_symbol(symbol)
        
        if not items:
            print(f"No specific news found for {symbol} in current feeds.")
            # Fallback to a general mention or just skip
            return pd.DataFrame()
            
        # Aggregate or take the latest
        # For simplicity, take the first one that matches and score it
        latest = items[0]
        text = latest['title'] + " " + latest['description']
        sentiment = self.analyze_sentiment(text)
        
        data = {
            'symbol': [symbol],
            'date': [datetime.date.today()],
            'sentiment_score': [sentiment],
            'material_event': [sentiment > 0.5 or sentiment < -0.5], # Arbitrary threshold
            'summary': [latest['title']]
        }
        return pd.DataFrame(data)
        
    def save_to_db(self, df):
        if df.empty:
            return
            
        conn = duckdb.connect(self.db_path)
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

