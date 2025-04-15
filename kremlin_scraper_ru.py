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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_ru.log'),
        logging.StreamHandler()
    ]
)

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
        self.supplements_data = {}

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse Russian date string to datetime object"""
        try:
            # Convert Russian month names to numbers
            ru_months = {
                'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            
            # Extract components from date string
            parts = date_str.strip().split()
            day = parts[0]
            month = ru_months[parts[1]]
            year = parts[2].replace('года', '').strip()
            
            # Create datetime object
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
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all news entries using the hentry class
            items = soup.find_all('div', class_=['hentry', 'h-entry', 'hentry_event'])
            
            for item in items:
                # Find the article link
                link = item.find('a', href=re.compile(r'/events/president/news/\d+'))
                if link:
                    article_url = f"{self.base_url}{link['href']}"
                    urls.append(article_url)
                    logging.info(f"Found article URL: {article_url}")
            
        except Exception as e:
            logging.error(f"Error getting articles from page {page_num}: {str(e)}")
        
        return urls

    def scrape_supplement(self, supplement_id: str) -> Dict:
        """Scrape data from a supplement page"""
        try:
            url = f"{self.base_url}/supplement/{supplement_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            title = soup.find('h1', class_='entry-title').text.strip() if soup.find('h1', class_='entry-title') else "No title"
            date_elem = soup.find('time', class_='read__published')
            date = self.parse_date(date_elem.text) if date_elem else None
            content = soup.find('div', class_='read__content')
            content_text = content.text.strip() if content else "No content"
            
            return {
                'title': title,
                'date': date,
                'content': content_text,
                'url': url
            }
        except Exception as e:
            logging.error(f"Error scraping supplement {supplement_id}: {str(e)}")
            return {}

    def scrape_article(self, url: str) -> Optional[Dict]:
        """Scrape a single article"""
        try:
            logging.info(f"Scraping article {url}")
            
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract main article data
            title_elem = soup.find('h1', class_='entry-title p-name')
            date_elem = soup.find('time', class_='read__published')
            place_elem = soup.find('div', class_='read__place p-location')
            lead_elem = soup.find('div', class_=['read__lead', 'entry-summary', 'p-summary'])
            
            # Check for supplements
            cut_title = soup.find('h3', class_='cut__title')
            supplements = []
            if cut_title:
                cut_items = soup.find_all('a', class_='cut__item')
                for item in cut_items:
                    href = item.get('href')
                    if href and '/supplement/' in href:
                        supplement_id = href.split('/')[-1]
                        supplement_data = self.scrape_supplement(supplement_id)
                        if supplement_data:
                            supplements.append(supplement_data)
                            self.supplements_data[supplement_id] = supplement_data
            
            if not title_elem:
                return None
                
            article_data = {
                'title': title_elem.text.strip() if title_elem else "No title",
                'date': self.parse_date(date_elem.text) if date_elem else None,
                'raw_date': date_elem.text.strip() if date_elem else "No date",
                'place': place_elem.text.strip() if place_elem else "No location",
                'summary': lead_elem.text.strip() if lead_elem else "No summary",
                'url': url,
                'supplements': supplements
            }
            
            logging.info(f"Successfully scraped article: {article_data['title']}")
            return article_data
            
        except Exception as e:
            logging.error(f"Error scraping article {url}: {str(e)}")
            return None

    def scrape_all_articles(self) -> pd.DataFrame:
        """Scrape all articles from page range"""
        articles_data = []
        
        # Go through pages in reverse order (newest to oldest)
        for page in range(self.start_page, self.end_page - 1, -1):
            # Get all article URLs from the current page
            article_urls = self.get_article_urls_from_page(page)
            
            # Scrape each article
            for url in article_urls:
                article_data = self.scrape_article(url)
                if article_data:
                    articles_data.append(article_data)
                time.sleep(2)  # Be respectful to the server
            
            time.sleep(2)  # Additional delay between pages
        
        # Convert to DataFrame
        df = pd.DataFrame(articles_data)
        
        # Save supplements to a separate file
        if self.supplements_data:
            with open('supplements_data.json', 'w', encoding='utf-8') as f:
                json.dump(self.supplements_data, f, ensure_ascii=False, indent=2, default=str)
        
        return df

def main():
    scraper = KremlinScraperRU()
    logging.info("Starting Kremlin RU scraper")
    
    try:
        df = scraper.scrape_all_articles()
        
        if not df.empty:
            # Save to CSV with proper encoding
            df.to_csv('kremlin_articles_ru.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Saved {len(df)} articles to kremlin_articles_ru.csv")
            
            # Print summary
            print("\nScraping Summary:")
            print(f"Total articles found: {len(df)}")
            if not df.empty:
                print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            print("\nFirst few entries:")
            print(tabulate(df[['title', 'date', 'place']].head(), 
                         headers='keys', tablefmt='grid', showindex=False))
            
            # Print supplements summary
            if scraper.supplements_data:
                print(f"\nTotal supplements collected: {len(scraper.supplements_data)}")
                print("Supplements data saved to supplements_data.json")
        else:
            logging.warning("No articles were found!")
            
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 