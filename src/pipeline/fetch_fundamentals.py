import os
import pandas as pd
import duckdb
import requests
from bs4 import BeautifulSoup
import json
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import datetime

load_dotenv()

class FundamentalsFetcher(ABC):
    @abstractmethod
    def fetch_fundamentals(self, symbol):
        pass

class TrendlyneFetcher(FundamentalsFetcher):
    def __init__(self):
        self.url_format = "https://trendlyne.com/equity/{symbol}/stock-page/"
        
    def fetch_fundamentals(self, symbol):
        print(f"Scraping fundamentals for {symbol} from Trendlyne...")
        url = self.url_format.format(symbol=symbol)
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
            if response.status_code != 200:
                print(f"Failed to load Trendlyne page for {symbol}. Status: {response.status_code}")
                return pd.DataFrame()
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try to extract JSON metrics from data attributes as seen in voyager repo
            key_metrics_div = soup.find('div', id="stock_key_metrics")
            perf_params_div = soup.find('div', id="stock_performance_parameters")
            
            def process_metrics(metrics_data):
                res = {}
                if isinstance(metrics_data, dict):
                    res.update(metrics_data)
                elif isinstance(metrics_data, list):
                    for item in metrics_data:
                        if isinstance(item, dict):
                            res.update(item)
                return res

            metrics = {}
            if key_metrics_div and key_metrics_div.has_attr('data-metrics'):
                metrics.update(process_metrics(json.loads(key_metrics_div['data-metrics'])))
            if perf_params_div and perf_params_div.has_attr('data-metrics'):
                metrics.update(process_metrics(json.loads(perf_params_div['data-metrics'])))
                
            if metrics:
                df = pd.DataFrame([{
                    'symbol': symbol,
                    'quarter': metrics.get('latest_quarter', 'N/A'),
                    'eps': float(metrics.get('eps', 0.0)),
                    'eps_growth_yoy': float(metrics.get('eps_growth_yoy', 0.0)),
                    'revenue': float(metrics.get('revenue', 0.0)),
                    'rev_growth_yoy': float(metrics.get('rev_growth_yoy', 0.0)),
                    'earnings_surprise': float(metrics.get('earnings_surprise', 0.0)),
                    'roe': float(metrics.get('roe', 0.0)),
                    'debt_to_equity': float(metrics.get('debt_to_equity', 0.0)),
                    'promoter_holding': float(metrics.get('promoter_holding', 0.0))
                }])
                return df
                
            # Fallback: Parse tables if JSON attributes not found
            print("JSON attributes not found. Parsing HTML tables...")
            # Implementation for parsing standard HTML tables on Trendlyne could go here if needed
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error scraping from Trendlyne: {e}")
            return pd.DataFrame()

class FundamentalsManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.fetcher = TrendlyneFetcher()
            
    def update_fundamentals(self, symbol):
        # Check cache: skip if fetched within last 7 days
        conn = duckdb.connect(self.db_path)
        try:
            res = conn.execute("SELECT MAX(fetch_date) FROM fundamentals WHERE symbol = ?", (symbol,)).fetchone()
            if res and res[0]:
                last_date = res[0]
                if isinstance(last_date, str):
                    last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
                elif isinstance(last_date, datetime.datetime):
                    last_date = last_date.date()
                
                if (datetime.date.today() - last_date).days < 7:
                    print(f"Fundamentals for {symbol} are up to date (last fetched {last_date}). Skipping.")
                    return
        except Exception as e:
            print(f"Error checking cache for {symbol}: {e}")
        finally:
            conn.close()

        df = self.fetcher.fetch_fundamentals(symbol)
        if not df.empty:
            self.save_to_db(df)
            
    def save_to_db(self, df):
        conn = duckdb.connect(self.db_path)
        try:
            # Add current date as fetch_date
            df['fetch_date'] = datetime.date.today()
            
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR REPLACE INTO fundamentals 
                SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, 
                       earnings_surprise, roe, debt_to_equity, promoter_holding, fetch_date
                FROM df_view
            """)
            print(f"Saved fundamentals for {df['symbol'].iloc[0]} to DB.")
        except Exception as e:
            print(f"Error saving fundamentals: {e}")
        finally:
            conn.close()
