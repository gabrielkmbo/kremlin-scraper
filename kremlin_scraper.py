import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
from fake_useragent import UserAgent
from tqdm import tqdm
import logging
import random
from typing import Dict, List, Optional
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

class KremlinScraper:
    def __init__(self):
        self.base_url = "http://en.kremlin.ru/events/president/news"
        self.ua = UserAgent()
        self.session = requests.Session()
        self.excluded_keywords = ['announcement', 'greeting', 'letter', 'telegram']
        
    def get_headers(self) -> Dict[str, str]:
        """Generate random headers for each request"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object"""
        try:
            # Handle various date formats
            date_str = date_str.strip()
            if ',' in date_str:
                date_str = date_str.split(',')[0]
            return datetime.strptime(date_str, '%B %d, %Y')
        except Exception as e:
            logging.error(f"Error parsing date {date_str}: {str(e)}")
            return None

    def extract_participants(self, content: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract participants and their positions from the content"""
        participants = []
        # Look for participant information in the content
        # This is a basic implementation - you might need to adjust based on actual HTML structure
        text = content.get_text()
        # Look for patterns like "Name, Position" or "Position Name"
        # This is a placeholder - you'll need to adjust the regex pattern based on actual content
        participant_pattern = r'([A-Za-z\s]+),\s*([A-Za-z\s]+)'
        matches = re.finditer(participant_pattern, text)
        
        for match in matches:
            participants.append({
                'name': match.group(1).strip(),
                'position': match.group(2).strip()
            })
        
        return participants

    def extract_meeting_details(self, url: str) -> Optional[Dict]:
        """Extract meeting details from a specific article page"""
        try:
            response = self.session.get(url, headers=self.get_headers(), timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title_elem = soup.find('h1', class_='entry-title')
            if not title_elem:
                logging.warning(f"No title found for {url}")
                return None
            title = title_elem.text.strip()
            
            # Extract date
            date_elem = soup.find('time')
            if not date_elem:
                logging.warning(f"No date found for {url}")
                return None
            date = self.parse_date(date_elem.text)
            if not date:
                return None
            
            # Extract content
            content = soup.find('div', class_='read__content')
            if not content:
                logging.warning(f"No content found for {url}")
                return None
            
            # Extract place (you'll need to adjust this based on actual HTML structure)
            place = "Not specified"  # Placeholder
            
            # Extract participants
            participants = self.extract_participants(content)
            
            return {
                'title': title,
                'date': date,
                'place': place,
                'participants': participants,
                'url': url
            }
            
        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")
            return None

    def scrape_meetings(self, start_date: datetime = None) -> pd.DataFrame:
        """Main scraping function"""
        if start_date is None:
            start_date = datetime.strptime('December 1, 2024', '%B %d, %Y')
        
        meetings_data = []
        page = 1
        
        with tqdm(desc="Scraping pages") as pbar:
            while True:
                try:
                    url = f"{self.base_url}?page={page}"
                    response = self.session.get(url, headers=self.get_headers(), timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find all news items
                    items = soup.find_all('div', class_='news-item')
                    if not items:
                        break
                    
                    for item in items:
                        title_elem = item.find('a')
                        if not title_elem:
                            continue
                            
                        title = title_elem.text.strip()
                        
                        # Skip unwanted content types
                        if any(keyword in title.lower() for keyword in self.excluded_keywords):
                            continue
                        
                        # Get the article URL and scrape details
                        article_url = f"http://en.kremlin.ru{title_elem['href']}"
                        meeting_data = self.extract_meeting_details(article_url)
                        
                        if meeting_data and meeting_data['date'] >= start_date:
                            meetings_data.append(meeting_data)
                        
                        # Random delay between requests (1-3 seconds)
                        time.sleep(random.uniform(1, 3))
                    
                    pbar.update(1)
                    page += 1
                    
                except Exception as e:
                    logging.error(f"Error on page {page}: {str(e)}")
                    break
        
        # Convert to DataFrame
        df = pd.DataFrame(meetings_data)
        return df

def main():
    scraper = KremlinScraper()
    logging.info("Starting Kremlin meetings scraper")
    
    try:
        df = scraper.scrape_meetings()
        
        # Save to CSV
        df.to_csv('kremlin_meetings.csv', index=False)
        logging.info(f"Successfully scraped {len(df)} meetings")
        
        # Print summary
        print("\nScraping Summary:")
        print(f"Total meetings scraped: {len(df)}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print("\nFirst few entries:")
        print(df.head())
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 