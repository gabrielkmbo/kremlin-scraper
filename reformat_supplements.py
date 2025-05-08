import pandas as pd
import json
import logging
import ast

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

INPUT_CSV = 'kremlin_articles_ru_enhanced.csv'
OUTPUT_CSV = 'kremlin_articles_ru_formatted.csv'

def safe_literal_eval(val):
    """Safely evaluate a string containing a Python literal (e.g., list, dict)."""
    if isinstance(val, str):
        try:
            # First try json.loads for standard JSON lists/dicts
            return json.loads(val.replace("'", '"')) # Handle single quotes common in stringified lists
        except json.JSONDecodeError:
            try:
                # Fallback to literal_eval for more complex Python structures if needed
                return ast.literal_eval(val)
            except (ValueError, SyntaxError, MemoryError, RecursionError) as e:
                logging.warning(f"Could not parse supplements string: {val}. Error: {e}")
                return [] # Return empty list if parsing fails
    elif isinstance(val, list): # Handle cases where it might already be a list
        return val
    return [] # Default to empty list for other types or errors

def process_row_supplements(supplements_str):
    """Extracts the URL of the first 'names' supplement and a combined list of all names."""
    supplements_list = safe_literal_eval(supplements_str)
    
    first_names_supplement_url = None
    combined_names_list = []

    if not isinstance(supplements_list, list):
        logging.warning(f"Supplements data is not a list: {supplements_str}")
        return pd.Series([first_names_supplement_url, combined_names_list])

    for supplement in supplements_list:
        if isinstance(supplement, dict) and supplement.get('type') == 'names':
            # Get URL of the first 'names' supplement encountered
            if first_names_supplement_url is None:
                first_names_supplement_url = supplement.get('url')
            
            # Combine names from all 'names' supplements
            names = supplement.get('names_list', [])
            if isinstance(names, list):
                combined_names_list.extend(names)
            else:
                logging.warning(f"'names_list' is not a list in supplement: {supplement}")

    return pd.Series([first_names_supplement_url, combined_names_list])

def main():
    logging.info(f"Reading input CSV: {INPUT_CSV}")
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        logging.error(f"Error: Input file '{INPUT_CSV}' not found.")
        return
    except Exception as e:
        logging.error(f"Error reading CSV '{INPUT_CSV}': {e}")
        return

    logging.info("Processing supplements column...")
    
    # Ensure supplements column exists
    if 'supplements' not in df.columns:
        logging.error(f"Error: 'supplements' column not found in '{INPUT_CSV}'.")
        return
        
    # Apply the function to parse supplements
    df[['supplement_url', 'extracted_names']] = df['supplements'].apply(process_row_supplements)

    # Find the maximum number of names to create columns
    max_names = df['extracted_names'].apply(len).max()
    logging.info(f"Maximum number of names found in any supplement list: {max_names}")

    # Create new 'Name_X' columns
    name_columns = [f'Name_{i+1}' for i in range(max_names)]
    for i, col_name in enumerate(name_columns):
        # Use .str[i] which safely handles lists and returns NaN if index is out of bounds
        df[col_name] = df['extracted_names'].str[i]

    logging.info(f"Created {len(name_columns)} 'Name_X' columns.")

    # Define final columns order (A-F original, G supplement_url, H+ Name_X)
    # Assuming original A-F are the first 6 columns
    original_cols = df.columns[:6].tolist() # Adjust if needed
    final_columns = original_cols + ['supplement_url'] + name_columns
    
    # Select and reorder columns, drop intermediate ones
    df_formatted = df[final_columns]

    logging.info(f"Saving formatted data to: {OUTPUT_CSV}")
    try:
        df_formatted.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        logging.info("Formatting complete.")
    except Exception as e:
        logging.error(f"Error writing CSV '{OUTPUT_CSV}': {e}")

if __name__ == "__main__":
    main() 