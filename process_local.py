"""
Direct CSV Processing Script for Testing

This script allows you to process the TechCorner_Sales_update.csv file directly
without requiring an SFTP server. Use this for local testing and development.
"""
import os
import logging
import pandas as pd
from datetime import datetime
from database import initialize_database, store_dataframe

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_csv(file_path):
    """Process a CSV file specifically for TechCorner sales data"""
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        logger.info(f"Read {len(df)} rows from CSV")
        
        # Print original column names for debugging
        logger.info(f"Original columns: {df.columns.tolist()}")
        
        # 1. Standardize column names (lowercase, replace spaces with underscores)
        df.columns = [col.lower().replace(' ', '_').replace('.', '_').replace('/', '_').replace('?', '') for col in df.columns]
        
        # 2. Specific column renaming for TechCorner data
        column_mapping = {
            'cus_id': 'customer_id',
            'cus__location': 'customer_location',
            'does_he_she_come_from_facebook_page': 'from_facebook',
            'does_he_she_followed_our_page': 'followed_page',
            'did_he_she_buy_any_mobile_before': 'previous_purchase',
            'did_he_she_hear_of_our_shop_before': 'heard_of_shop'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Print transformed column names for debugging
        logger.info(f"Transformed columns: {df.columns.tolist()}")
        
        # 3. Convert date to proper datetime format
        try:
            df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y', errors='coerce')
        except Exception as e:
            logger.warning(f"Could not convert date column to datetime: {str(e)}")
        
        # 4. Handle missing values appropriately
        for column in df.columns:
            if column in ['customer_id', 'age', 'sell_price']:
                # For numeric columns
                df[column] = df[column].fillna(0)
            elif column in ['from_facebook', 'followed_page', 'previous_purchase', 'heard_of_shop']:
                # For yes/no columns
                df[column] = df[column].fillna('Unknown')
                # Standardize yes/no values
                if df[column].dtype == object:  # Only process if it's a string column
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
        
        return df
    except Exception as e:
        logger.error(f"Failed to process CSV {file_path}: {str(e)}")
        return None

def main():
    """Process the TechCorner CSV file directly"""
    logger.info("Starting direct CSV processing")
    
    # Step 1: Initialize database
    logger.info("Initializing database")
    initialize_database()
    
    # Step 2: Process the CSV file
    file_path = os.getenv('CSV_FILE_PATH', './sample_data/TechCorner_Sales_update.csv')
    logger.info(f"Processing file: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        logger.info("Please download the TechCorner dataset from Kaggle and place it in the correct location.")
        return
    
    processed_data = process_csv(file_path)
    
    if processed_data is None:
        logger.error("Failed to process file")
        return
    
    logger.info(f"Successfully processed {len(processed_data)} rows")
    
    # Output a sample of the processed data
    if len(processed_data) > 0:
        logger.info("Sample of processed data:")
        print(processed_data.head(3).to_string())
    
    # Step 3: Store processed data in the database
    logger.info("Storing processed data in the database")
    success = store_dataframe(processed_data)
    
    if success:
        logger.info("Data successfully stored in the database")
        logger.info("You can now start the API server with: uvicorn api:app --reload --host 0.0.0.0 --port 8000")
    else:
        logger.error("Failed to store data in the database")

if __name__ == "__main__":
    main()