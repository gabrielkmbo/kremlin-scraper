# Kremlin Meetings Scraper

This script scrapes meeting information from the Kremlin's official website (kremlin.ru) and creates a structured dataset of meetings, including participants and their positions.

## Features

- Scrapes meetings from December 1, 2024 to present (up to 13 pages of content)
- Excludes announcements, greetings, letters, and telegrams
- Includes both in-person and virtual meetings
- Extracts meeting details including:
  - Title
  - Date with time
- Displays results in an easy-to-read table format
- Saves data to CSV format
- Includes logging and progress tracking

## Requirements

- Python 3.8 or higher
- Required packages listed in `requirements.txt`:
  - requests
  - beautifulsoup4
  - pandas
  - python-dateutil
  - fake-useragent
  - tqdm
  - tabulate

## Installation

1. Clone this repository
2. Create a virtual environment (recommended):
```bash
python -m venv kremlin_venv
source kremlin_venv/bin/activate  # On Windows: kremlin_venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:
```bash
python simple_kremlin_scraper.py
```

The script will:
1. Create a log file (`scraper.log`)
2. Save the scraped data to `kremlin_dates.csv`
3. Display a formatted table of all entries
4. Show progress and summary information

## Output

The script generates two files:
- `kremlin_dates.csv`: Contains the scraped meeting data (date and title)
- `scraper.log`: Contains detailed logging information

The script also displays results in a nicely formatted table in the terminal.

## Notes

- The script includes random delays between requests to be respectful to the website
- It uses rotating user agents to avoid detection
- Error handling and logging are implemented throughout the script
- The script respects the website's structure and includes proper error handling
- Limited to 13 pages of content to avoid overwhelming the server 