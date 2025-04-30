import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging
from tabulate import tabulate
import re
from typing import Dict, List, Optional
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

def clean_text(html_text: str) -> str:
    """Remove HTML tags, replace &nbsp;, and normalize whitespace."""
    if not html_text:
        return ""
    # Use BS4 to remove tags and handle entities
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text(separator=' ')
    # Replace non-breaking spaces and normalize whitespace
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

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
        # Removed self.supplements_data, will store directly in DataFrame

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse Russian date string (potentially with time) to datetime object"""
        try:
            ru_months = {
                'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            
            parts = date_str.strip().replace('года', '').split()
            day = parts[0]
            month = ru_months[parts[1]]
            year = parts[2]
            time_part = parts[3] if len(parts) > 3 else "00:00"
            
            # Try parsing with time, fallback to just date
            try:
                return datetime.strptime(f"{day}.{month}.{year} {time_part}", "%d.%m.%Y %H:%M")
            except ValueError:
                 return datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
        except Exception as e:
            logging.error(f"Error parsing date {date_str}: {str(e)}")
            return None

    def get_article_urls_from_page(self, page_num: int) -> List[str]:
        """Get all article URLs from a listing page"""
        urls = []
        try:
            url = f"{self.base_url}/events/president/news/page/{page_num}"
            if page_num == 1:
                url = f"{self.base_url}/events/president/news"
                
            logging.info(f"Scraping listing page {page_num}")
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('div', class_=['hentry', 'h-entry', 'hentry_event'])
            
            for item in items:
                link = item.find('a', href=re.compile(r'/events/president/news/\d+'))
                if link:
                    article_url = f"{self.base_url}{link['href']}"
                    urls.append(article_url)
                    # logging.info(f"Found article URL: {article_url}") # Reduced verbosity
            
        except Exception as e:
            logging.error(f"Error getting articles from page {page_num}: {str(e)}")
        
        return urls

    def scrape_supplement(self, supplement_url: str) -> Optional[Dict]:
        """Scrape data from a supplement page, detect name lists."""
        try:
            response = requests.get(supplement_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            title_elem = soup.find('h1', class_='entry-title')
            title = clean_text(str(title_elem)) if title_elem else "No title"
            
            content_div = soup.find('div', class_='read__content')
            if not content_div:
                return None # Or return basic info if needed

            # Extract potential names (LASTNAME F. M. - Position)
            # Regex: Capitalized word(s), space, Initial(s) or Name(s), space, dash, space, rest of line
            name_pattern = re.compile(r'^([А-ЯЁ\s]{2,})\s+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)\s*[–—-]?\s*(.*)', re.MULTILINE)
            
            names_list = []
            # Check paragraph by paragraph for better matching
            for p in content_div.find_all('p'):
                p_text = clean_text(str(p))
                match = name_pattern.match(p_text)
                if match:
                     # Check if the matched text looks like a name-position pair
                     # Simple heuristic: Contains capitalized last name and some position info
                     if len(match.group(1).split()) == 1 and len(match.group(3)) > 5: # Single capitalized word + position > 5 chars
                         names_list.append(p_text)
            
            # If names list found, return structured data
            if names_list:
                logging.info(f"Detected names list in supplement: {supplement_url}")
                return {
                    'type': 'names',
                    'url': supplement_url,
                    'title': title,
                    'names_list': names_list
                }
            else:
                # Otherwise, return cleaned full text content
                cleaned_content = clean_text(str(content_div))
                return {
                    'type': 'full_text',
                    'url': supplement_url,
                    'title': title,
                    'content': cleaned_content
                }

        except Exception as e:
            logging.error(f"Error scraping supplement {supplement_url}: {str(e)}")
            return None

    def scrape_article(self, url: str) -> Optional[Dict]:
        """Scrape a single article, including supplements."""
        try:
            logging.info(f"Scraping article {url}")
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            title_elem = soup.find('h1', class_='entry-title p-name')
            date_elem = soup.find('time', class_='read__published')
            place_elem = soup.find('div', class_='read__place p-location')
            lead_elem = soup.find('div', class_=['read__lead', 'entry-summary', 'p-summary'])
            
            if not title_elem:
                logging.warning(f"No title found for article {url}")
                return None
                
            # Scrape supplements
            supplements_list = []
            cut_items = soup.find_all('a', class_='cut__item', href=re.compile(r'/supplement/\d+'))
            for item in cut_items:
                supplement_url = f"{self.base_url}{item['href']}"
                supplement_data = self.scrape_supplement(supplement_url)
                if supplement_data:
                    supplements_list.append(supplement_data)
                time.sleep(1) # Small delay between supplement requests

            article_data = {
                'title': clean_text(str(title_elem)),
                'date': self.parse_date(date_elem.text) if date_elem else None,
                'raw_date': date_elem.text.strip() if date_elem else "No date",
                'place': clean_text(str(place_elem)) if place_elem else "No location",
                'summary': clean_text(str(lead_elem)) if lead_elem else "No summary",
                'url': url,
                'supplements': supplements_list # Store the list of supplement dicts
            }
            
            # logging.info(f"Successfully scraped article: {article_data['title']}") # Reduced verbosity
            return article_data
            
        except Exception as e:
            logging.error(f"Error scraping article {url}: {str(e)}")
            return None

    def scrape_all_articles(self) -> pd.DataFrame:
        """Scrape all articles from page range"""
        articles_data = []
        
        for page in range(self.start_page, self.end_page - 1, -1):
            article_urls = self.get_article_urls_from_page(page)
            
            for url in article_urls:
                article_data = self.scrape_article(url)
                if article_data:
                    articles_data.append(article_data)
                time.sleep(random.uniform(1.5, 3)) # Respectful delay
            
            time.sleep(random.uniform(2, 4)) # Delay between pages
        
        df = pd.DataFrame(articles_data)
        return df

def main():
    scraper = KremlinScraperRU()
    logging.info("Starting Kremlin RU scraper (page-based)")
    
    try:
        df = scraper.scrape_all_articles()
        
        if not df.empty:
            df.to_csv('kremlin_articles_ru.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Saved {len(df)} articles to kremlin_articles_ru.csv")
            
            print("\nScraping Summary:")
            print(f"Total articles found: {len(df)}")
            if 'date' in df.columns and not df['date'].isnull().all():
                 print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            else:
                 print("Date range: Not available (check parsing)")
                 
            print("\nFirst few entries (Title, Date, Place):")
            print(tabulate(df[['title', 'raw_date', 'place']].head(), 
                         headers='keys', tablefmt='grid', showindex=False))
            
            # Example of accessing supplements for the first row (if any)
            if not df.empty and 'supplements' in df.columns and df.iloc[0]['supplements']:
                print("\nSupplements example (first article):")
                print(json.dumps(df.iloc[0]['supplements'], ensure_ascii=False, indent=2, default=str))
        else:
            logging.warning("No articles were found!")
            
    except Exception as e:
        logging.error(f"Fatal error in main loop: {str(e)}")
        raise

if __name__ == "__main__":
    main() 