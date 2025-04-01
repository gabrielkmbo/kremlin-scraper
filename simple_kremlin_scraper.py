import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def should_include_entry(title):
    """Check if the entry should be included based on title"""
    # Convert title to lowercase for case-insensitive comparison
    title_lower = title.lower()
    
    # Keywords to exclude
    exclude_keywords = ['announcement', 'greeting', 'letter', 'telegram']
    
    # Keywords to explicitly include
    include_keywords = ['meeting', 'telephone conversation', 'phone call', 'talks', 'conference', 'virtual']
    
    # First check if it contains any exclude keywords
    if any(keyword in title_lower for keyword in exclude_keywords):
        return False
        
    # Then check if it contains any include keywords
    return any(keyword in title_lower for keyword in include_keywords)

def scrape_kremlin_dates():
    base_url = "http://en.kremlin.ru/events/president/news"
    dates_data = []
    page = 1
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    while True:
        try:
            url = f"{base_url}?page={page}"
            logging.info(f"Scraping page {page}")
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all news entries using the correct class
            items = soup.find_all('div', class_=['hentry', 'h-entry', 'hentry_event'])
            
            if not items:
                logging.info("No more items found")
                break
            
            for item in items:
                # Find the date element using the correct class
                date_elem = item.find('time', class_=['published', 'dt-published'])
                title_elem = item.find('span', class_=['entry-title', 'p-name'])
                location_elem = item.find('span', class_=['hentry__location', 'p-location'])
                
                if date_elem and title_elem:
                    title = title_elem.text.strip()
                    
                    # Skip if this entry should not be included
                    if not should_include_entry(title):
                        continue
                        
                    date_str = date_elem.text.strip()
                    try:
                        # Parse the date - the format is "April 1, 2025, 19:00"
                        date = datetime.strptime(date_str, '%B %d, %Y, %H:%M')
                        
                        entry = {
                            'date': date,
                            'raw_date': date_str,
                            'title': title,
                            'location': location_elem.text.strip() if location_elem else 'No location'
                        }
                        
                        dates_data.append(entry)
                        logging.info(f"Found entry: {date_str} - {entry['title']}")
                    except Exception as e:
                        logging.error(f"Error parsing date {date_str}: {str(e)}")
                        print(f"Raw date string: {date_str}")
            
            # Add a small delay between pages
            time.sleep(2)
            page += 1
            
        except Exception as e:
            logging.error(f"Error on page {page}: {str(e)}")
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(dates_data)
    
    if not df.empty:
        # Sort by date
        df = df.sort_values('date', ascending=False)
        
        # Save to CSV
        df.to_csv('kremlin_dates.csv', index=False)
        logging.info(f"Saved {len(df)} entries to kremlin_dates.csv")
        
        # Print summary
        print("\nScraping Summary:")
        print(f"Total entries found: {len(df)}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print("\nFirst few entries:")
        print(df.head())
    else:
        logging.warning("No entries were found!")

if __name__ == "__main__":
    scrape_kremlin_dates() 