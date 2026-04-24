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

class ScreenerFetcher:
    def __init__(self):
        self.url_format = "https://www.screener.in/company/{symbol}/"
        
    def fetch_fundamentals(self, symbol):
        print(f"Scraping fundamentals for {symbol} from Screener.in...")
        url = self.url_format.format(symbol=symbol)
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
            if response.status_code != 200:
                print(f"Failed to load Screener page for {symbol}. Status: {response.status_code}")
                return pd.DataFrame()
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 1. Extract from Profit & Loss table
            pl_section = soup.find('section', id='profit-loss')
            eps = 0.0
            eps_growth = 0.0
            revenue = 0.0
            rev_growth = 0.0
            
            if pl_section:
                table = pl_section.find('table', class_='data-table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if cells:
                            row_header = cells[0].get_text().strip()
                            if 'EPS in Rs' in row_header:
                                eps_str = cells[-1].get_text().strip()
                                try:
                                    eps = float(eps_str.replace(',', ''))
                                except ValueError:
                                    pass
                                if len(cells) >= 3:
                                    prev_eps_str = cells[-2].get_text().strip()
                                    try:
                                        prev_eps = float(prev_eps_str.replace(',', ''))
                                        if prev_eps != 0:
                                            eps_growth = ((eps - prev_eps) / prev_eps) * 100
                                    except ValueError:
                                        pass
                                        
                            elif 'Sales' in row_header or 'Revenue' in row_header:
                                rev_str = cells[-1].get_text().strip()
                                try:
                                    revenue = float(rev_str.replace(',', ''))
                                except ValueError:
                                    pass
                                if len(cells) >= 3:
                                    prev_rev_str = cells[-2].get_text().strip()
                                    try:
                                        prev_rev = float(prev_rev_str.replace(',', ''))
                                        if prev_rev != 0:
                                            rev_growth = ((revenue - prev_rev) / prev_rev) * 100
                                    except ValueError:
                                        pass
                                        
            # 2. Extract Promoter Holding from Shareholding table
            sh_section = soup.find('section', id='shareholding')
            promoter_holding = 0.0
            if sh_section:
                table = sh_section.find('table', class_='data-table')
                if table:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if cells:
                            row_header = cells[0].get_text().strip()
                            if 'Promoters' in row_header:
                                ph_str = cells[-1].get_text().strip()
                                try:
                                    promoter_holding = float(ph_str.replace('%', '').strip())
                                except ValueError:
                                    pass
                                    
            df = pd.DataFrame([{
                'symbol': symbol,
                'quarter': 'TTM',
                'eps': eps,
                'eps_growth_yoy': eps_growth,
                'revenue': revenue,
                'rev_growth_yoy': rev_growth,
                'earnings_surprise': 0.0,
                'roe': 0.0,
                'debt_to_equity': 0.0,
                'promoter_holding': promoter_holding
            }])
            return df
            
        except Exception as e:
            print(f"Error scraping from Screener: {e}")
            return pd.DataFrame()

class FundamentalsManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.fetcher = ScreenerFetcher()
            
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
