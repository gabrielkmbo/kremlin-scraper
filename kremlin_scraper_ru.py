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
        self.start_page = 138  # Changed from 10 to 138 for 2022
        self.end_page = 92     # Changed from 1 to 92 for 2022
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
            
            # Title: Prefer h1, potentially inside specific divs if class is not 'entry-title'
            title_elem = soup.find('h1', class_='entry-title') # Try standard first
            if not title_elem:
                read_top_div = soup.find('div', class_='read__top')
                if read_top_div:
                    title_elem = read_top_div.find('h1')
            title = clean_text(title_elem.text if title_elem else "")
            
            # Date: As per screenshot, it's in div.read_published
            # The existing date_elem = soup.find('time', class_='read__published') was likely for main articles.
            date_container_elem = soup.find('div', class_='read_published') 
            raw_date_supp = clean_text(date_container_elem.text if date_container_elem else None) # clean_text will handle nested fonts
            parsed_date_supp = self.parse_date(raw_date_supp)
            
            content_div = soup.find('div', class_='read__content')
            full_text = clean_text(str(content_div) if content_div else "")
            
            names_list = []
            if content_div:
                stronger_name_pattern = re.compile(r'^([А-ЯЁ\s]+[А-ЯЁ])\s*(?:–|-)\s*(.+)', re.MULTILINE)
                name_pattern = re.compile(r'^([А-ЯЁ][А-ЯЁа-яё\s\-]+[А-ЯЁ])\s+([А-ЯЁ][а-яё\.]*(?:\s+[А-ЯЁ][а-яё\.]*)?)\s*[–—-]?\s*(.*)', re.MULTILINE)
                for p_tag in content_div.find_all(['p', 'div']):
                    p_text = clean_text(p_tag.get_text(separator=' '))
                    if not p_text: continue
                    match = stronger_name_pattern.match(p_text)
                    if match and len(match.group(1).strip()) > 1 and len(match.group(2).strip()) > 3:
                        names_list.append(f"{match.group(1).strip()} – {match.group(2).strip()}")
                        continue
                    match = name_pattern.match(p_text)
                    if match:
                        last_name_candidate = match.group(1).strip()
                        if last_name_candidate.isupper() or (sum(1 for c in last_name_candidate if c.isupper()) / len(last_name_candidate.replace(" ","")) > 0.5 and len(last_name_candidate) > 3) :
                             names_list.append(p_text)
            
            return {
                'url': supplement_url,
                'title': title,
                'date': parsed_date_supp, # Use the date parsed from supplement page
                # 'raw_date' for supplement is removed as per request
                # 'place' for supplement is removed
                # 'summary' for supplement is removed
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
            raw_date_article = date_elem.text.strip() if date_elem else None
            parsed_date_article = self.parse_date(raw_date_article)
            place = clean_text(soup.find('div', class_='read__place p-location').text if soup.find('div', class_='read__place p-location') else "")
            summary = clean_text(soup.find('div', class_=['read__lead', 'entry-summary', 'p-summary']).text if soup.find('div', class_=['read__lead', 'entry-summary', 'p-summary']) else "")
            article_content_div = soup.find('div', class_='read__content')
            article_full_text = clean_text(str(article_content_div) if article_content_div else "")
            article_text_chunks = chunk_text(article_full_text, CHUNK_SIZE, MAX_TEXT_CHUNKS)

            article_data = {
                'title': title,
                'date': parsed_date_article,
                'raw_date': raw_date_article, # Main article's raw date
                'place': place,
                'summary': summary,
                **{f'article_full_text_{i+1}': chunk for i, chunk in enumerate(article_text_chunks)},
                'url': url,
                'supplement_url': None,
                'supplement_title': None,
                'supplement_date': None, # Parsed date for supplement
                # supplement_raw_date, supplement_place, supplement_summary removed
                 **{f'supplement_full_text_{i+1}': "" for i in range(MAX_TEXT_CHUNKS)},
                'names': []
            }

            supplement_links = soup.find_all('a', class_='cut__item', href=re.compile(r'/supplement/\d+'))
            if supplement_links:
                first_supplement_url = f"{self.base_url}{supplement_links[0]['href']}"
                scraped_supp_data = self.scrape_supplement(first_supplement_url)
                if scraped_supp_data:
                    article_data['supplement_url'] = scraped_supp_data['url']
                    article_data['supplement_title'] = scraped_supp_data['title']
                    article_data['supplement_date'] = scraped_supp_data['date'] # Assign parsed date
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
        """Scrape all articles from page range (start_page down to end_page)."""
        articles_data = []
        
        # Scrape pages from start_page down to end_page
        logging.info(f"--- Processing Pages {self.start_page} down to {self.end_page} for year 2022 ---")
        for page in range(self.start_page, self.end_page - 1, -1):
            logging.info(f"--- Processing Page {page} ---")
            article_urls = self.get_article_urls_from_page(page)
            for url in article_urls:
                article_data = self.scrape_article(url)
                if article_data:
                    articles_data.append(article_data)
                time.sleep(random.uniform(1.5, 3.0))
            time.sleep(random.uniform(2.0, 4.0))  # Delay between pages
        
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
    logging.info("Starting Kremlin RU scraper for year 2022 (pages 138-92)")
    try:
        df = scraper.scrape_all_articles()
        if not df.empty:
            core_cols = ['title', 'date', 'raw_date', 'place', 'summary']
            article_text_cols = [f'article_full_text_{i+1}' for i in range(MAX_TEXT_CHUNKS)]
            article_url_col = ['url']
            # Adjusted supplement core columns
            supp_core_cols = ['supplement_url', 'supplement_title', 'supplement_date']
            supp_text_cols = [f'supplement_full_text_{i+1}' for i in range(MAX_TEXT_CHUNKS)]
            name_cols = sorted([col for col in df.columns if col.startswith('Name_')]) 
            final_ordered_columns = core_cols + article_text_cols + article_url_col + \
                                    supp_core_cols + supp_text_cols + name_cols
            for col in final_ordered_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[final_ordered_columns]
            df.to_csv('kremlin_articles_ru_2022.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Saved {len(df)} articles to kremlin_articles_ru_2022.csv")
            print("\nScraping Summary:")
            print(f"Total articles found: {len(df)}")
            display_cols = ['title', 'raw_date', 'place', 'supplement_title', 'supplement_date']
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

if __name__ == "__main__":
    main() 