# Kremlin Meetings Scraper

This script scrapes meeting information from the Kremlin's official website (kremlin.ru) and creates a structured dataset of meetings, including participants and their positions.

## Features

- Scrapes meetings from December 1, 2024 to present
- Excludes announcements, greetings, letters, and telegrams
- Includes both in-person and virtual meetings
- Extracts meeting details including:
  - Title
  - Date
  - Place
  - Participants and their positions
- Saves data to CSV format
- Includes logging and progress tracking

## Requirements

- Python 3.8 or higher
- Required packages listed in `requirements.txt`

## Installation

1. Clone this repository
2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:
```bash
python kremlin_scraper.py
```

The script will:
1. Create a log file (`scraper.log`)
2. Save the scraped data to `kremlin_meetings.csv`
3. Display progress and summary information

## Output

The script generates two files:
- `kremlin_meetings.csv`: Contains the scraped meeting data
- `scraper.log`: Contains detailed logging information

## Notes

- The script includes random delays between requests to be respectful to the website
- It uses rotating user agents to avoid detection
- Error handling and logging are implemented throughout the script
- The script respects the website's structure and includes proper error handling 