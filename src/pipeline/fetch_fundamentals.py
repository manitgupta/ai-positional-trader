import os
import time
import pandas as pd
import duckdb
from config import connect_db
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

    def _scrape_top_ratios(self, soup):
        ratios = {}
        top_ratios = soup.find('ul', id='top-ratios')
        if top_ratios:
            items = top_ratios.find_all('li')
            for item in items:
                name_span = item.find('span', class_='name')
                value_span = item.find('span', class_='number')
                if name_span and value_span:
                    name = name_span.get_text().strip()
                    value = value_span.get_text().strip()
                    ratios[name] = value
        return ratios

    def _scrape_shareholding(self, soup):
        data = []
        shareholding_section = soup.find('section', id='shareholding')
        if not shareholding_section:
            return pd.DataFrame()
            
        table = shareholding_section.find('table', class_='data-table')
        if not table:
            return pd.DataFrame()
            
        headers_row = table.find('thead').find_all('tr')[0]
        quarter_names = [th.get_text().strip() for th in headers_row.find_all('th')[1:]]
        
        rows = table.find('tbody').find_all('tr')
        
        promoters_row = None
        fiis_row = None
        diis_row = None
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue
            header = cells[0].get_text().strip()
            data_cells = [c.get_text().strip() for c in cells[1:]]
            
            if header.startswith('Promoters'):
                promoters_row = data_cells
            elif header.startswith('FIIs'):
                fiis_row = data_cells
            elif header.startswith('DIIs'):
                diis_row = data_cells
                
        for i, quarter in enumerate(quarter_names):
            try:
                promoter = float(promoters_row[i].replace('%', '').replace(',', '').strip() or '0.0') if promoters_row and i < len(promoters_row) and promoters_row[i].strip() else 0.0
                fii = float(fiis_row[i].replace('%', '').replace(',', '').strip() or '0.0') if fiis_row and i < len(fiis_row) and fiis_row[i].strip() else 0.0
                dii = float(diis_row[i].replace('%', '').replace(',', '').strip() or '0.0') if diis_row and i < len(diis_row) and diis_row[i].strip() else 0.0
                
                data.append({
                    'quarter': parse_quarter(quarter),
                    'promoter_holding': promoter,
                    'fii_holding': fii,
                    'dii_holding': dii
                })
            except ValueError:
                pass
                
        return pd.DataFrame(data)


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
        
        ratios = self._scrape_top_ratios(soup)
        latest_roe = float(ratios.get('ROE', ratios.get('Return on Equity', '0.0')).replace('%', '').strip() or '0.0')
        latest_debt_to_equity = float(ratios.get('Debt to equity', '0.0').strip() or '0.0')
        latest_promoter_holding = float(ratios.get('Promoter holding', '0.0').replace('%', '').strip() or '0.0')
        
        data = []
        
        if pl_section:
            table = pl_section.find('table', class_='data-table')
            if table:
                thead = table.find('thead')
                if thead:
                    headers_row = thead.find_all('tr')[0]
                    header_names = [th.get_text().strip() for th in headers_row.find_all('th')[1:]]
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                    else:
                        rows = table.find_all('tr')[1:]
                else:
                    rows = table.find_all('tr')
                    if rows:
                        header_names = [th.get_text().strip() for th in rows[0].find_all(['th', 'td'])[1:]]
                        rows = rows[1:]
                    else:
                        header_names = []
                        rows = []
                        
                sales_row = None
                eps_row = None
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if not cells:
                        continue
                    header = cells[0].get_text().strip()
                    data_cells = [c.get_text().strip() for c in cells[1:]]
                    
                    if 'Sales' in header or 'Revenue' in header:
                        sales_row = data_cells
                    elif 'EPS in Rs' in header:
                        eps_row = data_cells
                        
                if sales_row and eps_row:
                    for i, col_name in enumerate(header_names):
                        try:
                            sales = float(sales_row[i].replace(',', '').strip() or '0.0') if i < len(sales_row) and sales_row[i].strip() else 0.0
                            eps = float(eps_row[i].replace(',', '').strip() or '0.0') if i < len(eps_row) and eps_row[i].strip() else 0.0
                            
                            data.append({
                                'symbol': symbol,
                                'quarter': col_name,
                                'eps': eps,
                                'revenue': sales,
                                'earnings_surprise': 0.0,
                                'roe': 0.0,
                                'debt_to_equity': 0.0,
                                'promoter_holding': 0.0
                            })
                        except ValueError:
                            pass
                            
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Compute YoY growth
        df['eps_growth_yoy'] = 0.0
        df['rev_growth_yoy'] = 0.0
        
        for i in range(1, len(df)):
            prev_eps = df['eps'].iloc[i-1]
            prev_rev = df['revenue'].iloc[i-1]
            
            if prev_eps != 0:
                df.loc[df.index[i], 'eps_growth_yoy'] = ((df['eps'].iloc[i] - prev_eps) / abs(prev_eps)) * 100
            if prev_rev != 0:
                df.loc[df.index[i], 'rev_growth_yoy'] = ((df['revenue'].iloc[i] - prev_rev) / abs(prev_rev)) * 100
                
        if not df.empty:
            df.loc[df.index[-1], 'roe'] = latest_roe
            df.loc[df.index[-1], 'debt_to_equity'] = latest_debt_to_equity
            df.loc[df.index[-1], 'promoter_holding'] = latest_promoter_holding
            
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
            shareholding_df = self._scrape_shareholding(soup)
            
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
                    sales = float(sales_row[i].replace(',', '').strip() or '0.0') if i < len(sales_row) and sales_row[i].strip() else 0.0
                    net_profit = float(net_profit_row[i].replace(',', '').strip() or '0.0') if i < len(net_profit_row) and net_profit_row[i].strip() else 0.0
                    eps = float(eps_row[i].replace(',', '').strip() or '0.0') if i < len(eps_row) and eps_row[i].strip() else 0.0
                    
                    data.append({
                        'symbol': symbol,
                        'quarter': parse_quarter(quarter),
                        'eps': eps,
                        'revenue': sales,
                        'net_profit': net_profit,
                        'earnings_surprise': 0.0
                    })
                except ValueError:
                    pass
                    
            df = pd.DataFrame(data)
            
            if not shareholding_df.empty:
                df = df.merge(shareholding_df, on='quarter', how='left')
            
            for col in ['promoter_holding', 'fii_holding', 'dii_holding']:
                if col not in df.columns:
                    df[col] = 0.0
                else:
                    df[col] = df[col].fillna(0.0)
            
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
        
    def update_fundamentals(self, symbol, force=False):
        self.update_annual_data(symbol, force=force)
        self.update_quarterly_data(symbol, force=force)
            
    def update_annual_data(self, symbol, force=False):
        # Check cache: skip if fetched within last 30 days for annual
        if not force:
            conn = connect_db(self.db_path)
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
            
    def update_quarterly_data(self, symbol, force=False):
        # Check cache: skip if fetched within last 7 days for quarterly
        if not force:
            conn = connect_db(self.db_path)
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
        conn = connect_db(self.db_path)
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
        conn = connect_db(self.db_path)
        try:
            df['fetch_date'] = datetime.date.today()
            conn.register('df_view', df)
            conn.execute("""
                INSERT OR REPLACE INTO quarterly_results 
                SELECT symbol, quarter, eps, eps_growth_yoy, revenue, rev_growth_yoy, 
                       net_profit, fetch_date, promoter_holding, fii_holding, dii_holding
                FROM df_view
            """)
            print(f"Saved quarterly data for {df['symbol'].iloc[0]} to DB.")
        except Exception as e:
            print(f"Error saving quarterly data: {e}")
        finally:
            conn.close()
