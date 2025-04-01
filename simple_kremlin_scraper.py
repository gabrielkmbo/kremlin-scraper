import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging
from tabulate import tabulate  # for nice table formatting

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def scrape_kremlin_dates():
    base_url = "http://en.kremlin.ru/events/president/news"
    dates_data = []
    max_pages = 13
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    for page in range(1, max_pages + 1):
        try:
            # Use the correct URL format
            url = f"{base_url}/page/{page}" if page > 1 else base_url
            logging.info(f"Scraping page {page} of {max_pages}")
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all news entries using the correct class
            items = soup.find_all('div', class_=['hentry', 'h-entry', 'hentry_event'])
            
            if not items:
                logging.info("No items found on this page")
                break
            
            for item in items:
                # Find the date element using the correct class
                date_elem = item.find('time', class_=['published', 'dt-published'])
                title_elem = item.find('span', class_=['entry-title', 'p-name'])
                
                if date_elem and title_elem:
                    date_str = date_elem.text.strip()
                    try:
                        # Parse the date - the format is "April 1, 2025, 19:00"
                        date = datetime.strptime(date_str, '%B %d, %Y, %H:%M')
                        
                        entry = {
                            'Date': date_str,
                            'Title': title_elem.text.strip()
                        }
                        
                        dates_data.append(entry)
                        logging.info(f"Found entry: {date_str} - {entry['Title']}")
                    except Exception as e:
                        logging.error(f"Error parsing date {date_str}: {str(e)}")
            
            # Add a small delay between pages
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error on page {page}: {str(e)}")
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(dates_data)
    
    if not df.empty:
        # Save to CSV
        df.to_csv('kremlin_dates.csv', index=False)
        logging.info(f"Saved {len(df)} entries to kremlin_dates.csv")
        
        # Print table using tabulate
        print("\nKremlin News Entries:")
        print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        
        # Print summary
        print(f"\nTotal entries found: {len(df)}")
    else:
        logging.warning("No entries were found!")

if __name__ == "__main__":
    scrape_kremlin_dates() 