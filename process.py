"""
Data Processing Module
"""
import pandas as pd
import json
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_csv(file_path):
    """Process a CSV file specifically for TechCorner sales data"""
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Basic cleaning operations
        # 1. Standardize column names (lowercase, replace spaces with underscores)
        df.columns = [col.lower().replace(' ', '_').replace('.', '_').replace('/', '_') for col in df.columns]
        
        # 2. Specific column renaming for TechCorner data
        column_mapping = {
            'cus_id': 'customer_id',
            'cus__location': 'customer_location',
            'does_he_she_come_from_facebook_page_': 'from_facebook',
            'does_he_she_followed_our_page_': 'followed_page',
            'did_he_she_buy_any_mobile_before_': 'previous_purchase',
            'did_he_she_hear_of_our_shop_before_': 'heard_of_shop'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # 3. Convert date to proper datetime format
        try:
            df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        except:
            logging.warning(f"Could not convert date column to datetime in file: {file_path}")
        
        # 4. Handle missing values appropriately
        for column in df.columns:
            if column in ['customer_id', 'age', 'sell_price']:
                # For numeric columns
                df[column] = df[column].fillna(0)
            elif column in ['from_facebook', 'followed_page', 'previous_purchase', 'heard_of_shop']:
                # For yes/no columns
                df[column] = df[column].fillna('Unknown')
                # Standardize yes/no values
                df[column] = df[column].str.lower().replace({'yes': 'Yes', 'no': 'No', 'y': 'Yes', 'n': 'No'})
            else:
                # For other string columns
                df[column] = df[column].fillna('Unknown')
        
        # 5. Clean gender column
        if 'gender' in df.columns:
            df['gender'] = df['gender'].str.strip().str.capitalize()
        
        # 6. Add processing metadata
        df['source_file'] = os.path.basename(file_path)
        df['processed_at'] = datetime.now()
        
        logging.info(f"Successfully processed CSV file: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error processing CSV file {file_path}: {str(e)}")
        return None


def process_json(file_path):
    """Process a JSON file"""
    try:
        # Read JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, list):
            # List of objects
            df = pd.json_normalize(data)
        elif isinstance(data, dict):
            # Single object or nested structure
            df = pd.json_normalize(data)
        else:
            logging.error(f"Unsupported JSON structure in {file_path}")
            return None
        
        # Standardize column names
        df.columns = [col.lower().replace('.', '_').replace(' ', '_') for col in df.columns]
        
        # Handle missing values
        for column in df.columns:
            if pd.api.types.is_numeric_dtype(df[column]):
                df[column] = df[column].fillna(0)
            else:
                df[column] = df[column].fillna('Unknown')
        
        # Add processing metadata
        df['source_file'] = os.path.basename(file_path)
        df['processed_at'] = datetime.now()
        
        logging.info(f"Successfully processed JSON file: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error processing JSON file {file_path}: {str(e)}")
        return None


def process_file(file_path):
    """Process a file based on its extension"""
    if file_path.endswith('.csv'):
        return process_csv(file_path)
    elif file_path.endswith('.json'):
        return process_json(file_path)
    else:
        logging.warning(f"Unsupported file format: {file_path}")
        return None


def process_files(file_paths):
    """Process multiple files"""
    dataframes = []

    for file_path in file_paths:
        df = process_file(file_path)
        if df is not None:
            dataframes.append(df)

    if dataframes:
        # Combine all dataframes
        try:
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df
        except Exception as e:
            logging.error(f"Error combining dataframes: {str(e)}")
            return None
    
    return None


# For testing
if __name__ == "__main__":
    # Test with sample file
    files = [
        './sample_data/TechCorner_Sales_update.csv'
    ]
    
    result = process_files(files)
    if result is not None:
        print(f"Processed {len(result)} rows")
        print(result.head())