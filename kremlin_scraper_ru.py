import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging
from tabulate import tabulate
import re
from typing import Dict, List, Optional, Tuple
import json
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_ru.log'),
        logging.StreamHandler()
    ]
)

MAX_TEXT_CHUNKS = 10
CHUNK_SIZE = 30000

def clean_text(html_text: Optional[str]) -> str:
    """Remove HTML tags, replace &nbsp;, and normalize whitespace."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text(separator=' ')
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def chunk_text(text: str, chunk_size: int, max_chunks: int) -> List[str]:
    """Splits text into a list of chunks, up to max_chunks."""
    if not text:
        return [""] * max_chunks # Return list of empty strings if no text
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    # Pad with empty strings if fewer chunks than max_chunks
    return (chunks + [""] * max_chunks)[:max_chunks]

class KremlinScraperRU:
    def __init__(self):
        self.base_url = "http://kremlin.ru"
        self.start_page = 10
        self.end_page = 1
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
        }

    def parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            ru_months = {
                'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            parts = date_str.strip().replace('года', '').split()
            if len(parts) < 3: # Ensure day, month, year are present
                logging.warning(f"Date string too short to parse: {date_str}")
                return None
            day = parts[0]
            month_ru = parts[1].lower()
            if month_ru not in ru_months:
                logging.warning(f"Unknown Russian month: {parts[1]} in {date_str}")
                return None
            month = ru_months[month_ru]
            year = parts[2]
            time_part = parts[3] if len(parts) > 3 else "00:00"
            
            try:
                return datetime.strptime(f"{day}.{month}.{year} {time_part}", "%d.%m.%Y %H:%M")
            except ValueError:
                 return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
        except Exception as e:
            logging.error(f"Error parsing date '{date_str}': {str(e)}")
            return None

    def get_article_urls_from_page(self, page_num: int) -> List[str]:
        urls = []
        page_url = f"{self.base_url}/events/president/news/page/{page_num}" if page_num > 1 else f"{self.base_url}/events/president/news"
        logging.info(f"Scraping listing page {page_num} ({page_url})")
        try:
            response = requests.get(page_url, headers=self.headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_=['hentry', 'h-entry', 'hentry_event'])
            for item in items:
                link = item.find('a', href=re.compile(r'/events/president/news/\d+'))
                if link and link['href']:
                    urls.append(f"{self.base_url}{link['href']}")
        except Exception as e:
            logging.error(f"Error getting articles from page {page_num} ({page_url}): {str(e)}")
        return urls

    def scrape_supplement(self, supplement_url: str) -> Optional[Dict]:
        logging.info(f"Scraping supplement: {supplement_url}")
        try:
            response = requests.get(supplement_url, headers=self.headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            title = clean_text(soup.find('h1', class_='entry-title').text if soup.find('h1', class_='entry-title') else "")
            
            date_elem = soup.find('time', class_='read__published') # Common on supplement pages too
            raw_date = date_elem.text.strip() if date_elem else None
            parsed_date = self.parse_date(raw_date)
            
            content_div = soup.find('div', class_='read__content')
            full_text = clean_text(str(content_div) if content_div else "")
            
            names_list = []
            if content_div:
                # Regex: Capitalized word(s), space, Name(s) or Initial(s), space, optional dash, space, rest of line
                # Making it more flexible for various formats observed.
                name_pattern = re.compile(r'^([А-ЯЁ][А-ЯЁа-яё\s\-]+[А-ЯЁ])\s+([А-ЯЁ][а-яё\.]*(?:\s+[А-ЯЁ][а-яё\.]*)?)\s*[–—-]?\s*(.*)', re.MULTILINE)
                # Simpler pattern focusing on ALL CAPS LAST NAME - Position (often more reliable)
                # Pattern: ALL CAPS WORD(S) (Last Name) potentially with spaces, then optional First/Middle, then - Position
                stronger_name_pattern = re.compile(r'^([А-ЯЁ\s]+[А-ЯЁ])\s*(?:–|-)\s*(.+)', re.MULTILINE)

                for p_tag in content_div.find_all(['p', 'div']): # Check divs too, sometimes lists are in divs
                    p_text = clean_text(p_tag.get_text(separator=' '))
                    if not p_text: continue

                    # Try stronger pattern first
                    match = stronger_name_pattern.match(p_text)
                    if match and len(match.group(1).strip()) > 1 and len(match.group(2).strip()) > 3: # Basic check for validity
                        names_list.append(f"{match.group(1).strip()} – {match.group(2).strip()}")
                        continue # Found with stronger pattern
                    
                    # Fallback to the more general pattern if the stronger one fails
                    match = name_pattern.match(p_text)
                    if match:
                        last_name_candidate = match.group(1).strip()
                        # Check if last_name_candidate is mostly uppercase and likely a surname
                        if last_name_candidate.isupper() or (sum(1 for c in last_name_candidate if c.isupper()) / len(last_name_candidate.replace(" ","")) > 0.5 and len(last_name_candidate) > 3) :
                             names_list.append(p_text) # Keep original format from text
            
            return {
                'url': supplement_url,
                'title': title,
                'date': parsed_date,
                'raw_date': raw_date,
                'place': None, # Supplements don't typically have a distinct place
                'summary': None, # Supplements don't typically have a distinct summary
                'full_text': full_text,
                'names_list': names_list if names_list else None
            }
        except Exception as e:
            logging.error(f"Error scraping supplement {supplement_url}: {str(e)}")
            return None

    def scrape_article(self, url: str) -> Optional[Dict]:
        logging.info(f"Scraping article: {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            title = clean_text(soup.find('h1', class_='entry-title p-name').text if soup.find('h1', class_='entry-title p-name') else "")
            
            date_elem = soup.find('time', class_='read__published')
            raw_date = date_elem.text.strip() if date_elem else None
            parsed_date = self.parse_date(raw_date)
            
            place = clean_text(soup.find('div', class_='read__place p-location').text if soup.find('div', class_='read__place p-location') else "")
            summary = clean_text(soup.find('div', class_=['read__lead', 'entry-summary', 'p-summary']).text if soup.find('div', class_=['read__lead', 'entry-summary', 'p-summary']) else "")
            
            article_content_div = soup.find('div', class_='read__content')
            article_full_text = clean_text(str(article_content_div) if article_content_div else "")
            article_text_chunks = chunk_text(article_full_text, CHUNK_SIZE, MAX_TEXT_CHUNKS)

            # Initialize supplement fields with defaults
            supplement_data = {
                f'full_text_{i+1}': "" for i in range(MAX_TEXT_CHUNKS)
            }
            article_data = {
                'title': title,
                'date': parsed_date,
                'raw_date': raw_date,
                'place': place,
                'summary': summary,
                **{f'article_full_text_{i+1}': chunk for i, chunk in enumerate(article_text_chunks)},
                'url': url,
                'supplement_url': None,
                'supplement_title': None,
                'supplement_date': None,
                'supplement_raw_date': None,
                'supplement_place': None,
                'supplement_summary': None,
                 **{f'supplement_full_text_{i+1}': "" for i in range(MAX_TEXT_CHUNKS)},
                'names': [] # Store extracted name list here
            }

            # Process first supplement
            supplement_links = soup.find_all('a', class_='cut__item', href=re.compile(r'/supplement/\d+'))
            if supplement_links:
                first_supplement_url = f"{self.base_url}{supplement_links[0]['href']}"
                scraped_supp_data = self.scrape_supplement(first_supplement_url)
                if scraped_supp_data:
                    article_data['supplement_url'] = scraped_supp_data['url']
                    article_data['supplement_title'] = scraped_supp_data['title']
                    article_data['supplement_date'] = scraped_supp_data['date']
                    article_data['supplement_raw_date'] = scraped_supp_data['raw_date']
                    # supplement_place and supplement_summary are intentionally None from scrape_supplement
                    supplement_text_chunks = chunk_text(scraped_supp_data['full_text'], CHUNK_SIZE, MAX_TEXT_CHUNKS)
                    for i, chunk in enumerate(supplement_text_chunks):
                        article_data[f'supplement_full_text_{i+1}'] = chunk
                    if scraped_supp_data['names_list']:
                        article_data['names'] = scraped_supp_data['names_list']
            
            return article_data
        except Exception as e:
            logging.error(f"Error scraping article {url}: {str(e)}")
            return None

    def scrape_all_articles(self) -> pd.DataFrame:
        articles_data = []
        logging.info("--- Processing Page 1 ---")
        article_urls_p1 = self.get_article_urls_from_page(1)
        for url in article_urls_p1:
            article_data = self.scrape_article(url)
            if article_data:
                articles_data.append(article_data)
            time.sleep(random.uniform(1.5, 3.0))
        
        if self.start_page >= 2:
            logging.info(f"--- Processing Pages {self.start_page} down to 2 ---")
            for page in range(self.start_page, 1, -1):
                logging.info(f"--- Processing Page {page} ---")
                article_urls = self.get_article_urls_from_page(page)
                for url in article_urls:
                    article_data = self.scrape_article(url)
                    if article_data:
                        articles_data.append(article_data)
                    time.sleep(random.uniform(1.5, 3.0))
                time.sleep(random.uniform(2.0, 4.0))
        
        df = pd.DataFrame(articles_data)

        # Dynamically create Name_X columns
        if not df.empty and 'names' in df.columns:
            max_names_found = df['names'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()
            if max_names_found > 0:
                logging.info(f"Max names in a supplement: {max_names_found}. Creating Name_X columns.")
                for i in range(max_names_found):
                    df[f'Name_{i+1}'] = df['names'].apply(lambda x: x[i] if isinstance(x, list) and i < len(x) else None)
            df = df.drop(columns=['names']) # Drop the temporary list column
        elif 'names' in df.columns: # If column exists but was empty
             df = df.drop(columns=['names'])

        return df

def main():
    scraper = KremlinScraperRU()
    logging.info("Starting Kremlin RU scraper (enhanced with full text & detailed supplements)")
    try:
        df = scraper.scrape_all_articles()
        if not df.empty:
            # Define column order carefully
            core_cols = ['title', 'date', 'raw_date', 'place', 'summary']
            article_text_cols = [f'article_full_text_{i+1}' for i in range(MAX_TEXT_CHUNKS)]
            article_url_col = ['url']
            supp_core_cols = ['supplement_url', 'supplement_title', 'supplement_date', 'supplement_raw_date', 'supplement_place', 'supplement_summary']
            supp_text_cols = [f'supplement_full_text_{i+1}' for i in range(MAX_TEXT_CHUNKS)]
            
            # Get dynamic Name_X columns that were created
            name_cols = sorted([col for col in df.columns if col.startswith('Name_')]) 
            
            final_ordered_columns = core_cols + article_text_cols + article_url_col + \
                                    supp_core_cols + supp_text_cols + name_cols
            
            # Ensure all expected columns exist in df, add if missing (e.g. if no names found, Name_X cols won't exist yet)
            for col in final_ordered_columns:
                if col not in df.columns:
                    df[col] = None # or pd.NA or ""
            
            df = df[final_ordered_columns] # Reorder and select

            df.to_csv('kremlin_articles_ru_enhanced.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Saved {len(df)} articles to kremlin_articles_ru_enhanced.csv")
            print("\nScraping Summary:")
            print(f"Total articles found: {len(df)}")
            
            display_cols = ['title', 'raw_date', 'place', 'supplement_title']
            # Ensure display_cols exist before trying to print them
            display_cols_present = [col for col in display_cols if col in df.columns]
            if display_cols_present:
                 print("\nFirst few entries (selected columns):")
                 print(tabulate(df[display_cols_present].head(), headers='keys', tablefmt='grid', showindex=False))
            else:
                 print("\nFirst few entries (title only):")
                 print(tabulate(df[['title']].head(), headers='keys', tablefmt='grid', showindex=False))

        else:
            logging.warning("No articles were found!")
    except Exception as e:
        logging.error(f"Fatal error in main loop: {str(e)}", exc_info=True)
        # raise # Optional: re-raise after logging for more detailed traceback if debugging

if __name__ == "__main__":
    main() 