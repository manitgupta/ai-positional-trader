import os
import time
import pandas as pd
import duckdb
import requests
from bs4 import BeautifulSoup
import json
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import datetime

load_dotenv()

def parse_quarter(q_str):
    parts = q_str.split()
    if len(parts) == 2:
        month, year = parts
        month_map = {'Mar': '03', 'Jun': '06', 'Sep': '09', 'Dec': '12'}
        return f"{year}-{month_map.get(month, '00')}"
    return q_str

class ScreenerFetcher:
    def __init__(self):
        self.url_format = "https://www.screener.in/company/{symbol}/"
        
    def _get_page(self, url, headers):
        delay = 2
        for attempt in range(3):
            try:
                response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    print(f"Rate limited (429). Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    print(f"Status {response.status_code}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
        return None

    def fetch_annual_data(self, symbol):
        print(f"Scraping annual fundamentals for {symbol} from Screener.in...")
        url = f"https://www.screener.in/company/{symbol}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = self._get_page(url, headers)
        if not response or response.status_code != 200:
             return pd.DataFrame()
             
        soup = BeautifulSoup(response.content, 'html.parser')
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
            'promoter_holding': 0.0
        }])
        return df

    def fetch_quarterly_data(self, symbol):
        print(f"Scraping fundamentals for {symbol} from Screener.in...")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        urls = [
            f"https://www.screener.in/company/{symbol}/consolidated/",
            f"https://www.screener.in/company/{symbol}/"
        ]
        
        for url in urls:
            print(f"Trying URL: {url}")
            df = self._fetch_from_url(symbol, url, headers)
            if not df.empty:
                return df
                
        print(f"Failed to load valid quarterly data for {symbol} from any URL.")
        return pd.DataFrame()
        
    def _fetch_from_url(self, symbol, url, headers):
        try:
            response = self._get_page(url, headers)
            if not response or response.status_code != 200:
                return pd.DataFrame()
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find Quarterly Results section
            quarters_section = soup.find('section', id='quarters')
            if not quarters_section:
                return pd.DataFrame()
                
            table = quarters_section.find('table', class_='data-table')
            if not table:
                return pd.DataFrame()
                
            # Extract headers (quarters)
            headers_row = table.find('thead').find_all('tr')[0]
            quarter_names = [th.get_text().strip() for th in headers_row.find_all('th')[1:]]
            
            # Extract rows
            rows = table.find('tbody').find_all('tr')
            sales_row = None
            net_profit_row = None
            eps_row = None
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue
                header = cells[0].get_text().strip()
                
                # Get data cells (all cells except the first one, which is the header)
                data_cells = [c.get_text().strip() for c in cells[1:]]
                
                if 'Sales' in header or 'Revenue' in header:
                    sales_row = data_cells
                elif 'Net Profit' in header:
                    net_profit_row = data_cells
                elif 'EPS in Rs' in header:
                    eps_row = data_cells
                    
            if not sales_row or not net_profit_row or not eps_row:
                return pd.DataFrame()
                
            # Check if data cells are empty or missing numbers
            if len(sales_row) == 0 or all(x == '' for x in sales_row):
                return pd.DataFrame()
                
            # Create DataFrame
            data = []
            for i, quarter in enumerate(quarter_names):
                try:
                    sales = float(sales_row[i].replace(',', '')) if i < len(sales_row) and sales_row[i] else 0.0
                    net_profit = float(net_profit_row[i].replace(',', '')) if i < len(net_profit_row) and net_profit_row[i] else 0.0
                    eps = float(eps_row[i].replace(',', '')) if i < len(eps_row) and eps_row[i] else 0.0
                    
                    data.append({
                        'symbol': symbol,
                        'quarter': parse_quarter(quarter),
                        'eps': eps,
                        'revenue': sales,
                        'net_profit': net_profit,
                        'earnings_surprise': 0.0,
                        'roe': 0.0,
                        'debt_to_equity': 0.0,
                        'promoter_holding': 0.0
                    })
                except ValueError:
                    pass
                    
            df = pd.DataFrame(data)
            
            # Compute YoY growth
            df['eps_growth_yoy'] = 0.0
            df['rev_growth_yoy'] = 0.0
            
            for i in range(4, len(df)):
                prev_eps = df['eps'].iloc[i-4]
                prev_rev = df['revenue'].iloc[i-4]
                
                if prev_eps != 0:
                    df.loc[df.index[i], 'eps_growth_yoy'] = ((df['eps'].iloc[i] - prev_eps) / abs(prev_eps)) * 100
                if prev_rev != 0:
                    df.loc[df.index[i], 'rev_growth_yoy'] = ((df['revenue'].iloc[i] - prev_rev) / abs(prev_rev)) * 100
                    
            return df
            
        except Exception as e:
            print(f"Error parsing URL {url}: {e}")
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error scraping from Screener: {e}")
            return pd.DataFrame()

class FundamentalsManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.fetcher = ScreenerFetcher()
        
    def update_fundamentals(self, symbol):
        self.update_annual_data(symbol)
        self.update_quarterly_data(symbol)
            
    def update_annual_data(self, symbol):
        # Check cache: skip if fetched within last 30 days for annual
        conn = duckdb.connect(self.db_path)
        try:
            res = conn.execute("SELECT MAX(fetch_date) FROM annual_results WHERE symbol = ?", (symbol,)).fetchone()
            if res and res[0]:
                last_date = res[0]
                if isinstance(last_date, str):
                    last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
                elif isinstance(last_date, datetime.datetime):
                    last_date = last_date.date()
                
                if (datetime.date.today() - last_date).days < 30:
                    print(f"Annual data for {symbol} is up to date. Skipping.")
                    return
        except Exception as e:
            print(f"Error checking cache for {symbol}: {e}")
        finally:
            conn.close()

        df = self.fetcher.fetch_annual_data(symbol)
        if not df.empty:
            self.save_annual_to_db(df)
            
    def update_quarterly_data(self, symbol):
        # Check cache: skip if fetched within last 7 days for quarterly
        conn = duckdb.connect(self.db_path)
        try:
            res = conn.execute("SELECT MAX(fetch_date) FROM quarterly_results WHERE symbol = ?", (symbol,)).fetchone()
            if res and res[0]:
                last_date = res[0]
                if isinstance(last_date, str):
                    last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
                elif isinstance(last_date, datetime.datetime):
                    last_date = last_date.date()
                
                if (datetime.date.today() - last_date).days < 7:
                    print(f"Quarterly data for {symbol} is up to date. Skipping.")
                    return
        except Exception as e:
            print(f"Error checking cache for {symbol}: {e}")
        finally:
            conn.close()

        df = self.fetcher.fetch_quarterly_data(symbol)
        if not df.empty:
            self.save_quarterly_to_db(df)
            
    def save_annual_to_db(self, df):
        conn = duckdb.connect(self.db_path)
        try:
            df['fetch_date'] = datetime.date.today()
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR REPLACE INTO annual_results 
                SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, 
                       earnings_surprise, roe, debt_to_equity, promoter_holding, fetch_date
                FROM df_view
            """)
            print(f"Saved annual data for {df['symbol'].iloc[0]} to DB.")
        except Exception as e:
            print(f"Error saving annual data: {e}")
        finally:
            conn.close()

    def save_quarterly_to_db(self, df):
        conn = duckdb.connect(self.db_path)
        try:
            df['fetch_date'] = datetime.date.today()
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR REPLACE INTO quarterly_results 
                SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, 
                       net_profit, fetch_date
                FROM df_view
            """)
            print(f"Saved quarterly data for {df['symbol'].iloc[0]} to DB.")
        except Exception as e:
            print(f"Error saving quarterly data: {e}")
        finally:
            conn.close()
