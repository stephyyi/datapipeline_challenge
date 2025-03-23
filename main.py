"""
Main entry point for the data pipeline
"""
import os
import logging
import argparse
from datetime import datetime

from ingest import ingest_data
from process import process_files
from database import initialize_database, store_dataframe

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_pipeline(sftp_config):
    """Run the complete data pipeline"""
    logger.info("Starting data pipeline")
    
    # Step 1: Initialize database
    logger.info("Initializing database")
    initialize_database()
    
    # Step 2: Ingest data from SFTP
    logger.info("Ingesting data from SFTP")
    downloaded_files = ingest_data(sftp_config)
    
    if not downloaded_files:
        logger.warning("No files were downloaded. Pipeline stopped.")
        return
    
    logger.info(f"Downloaded {len(downloaded_files)} files")
    
    # Step 3: Process the downloaded files
    logger.info("Processing files")
    processed_data = process_files(downloaded_files)
    
    if processed_data is None:
        logger.error("Failed to process files. Pipeline stopped.")
        return
    
    logger.info(f"Processed {len(processed_data)} rows of data")
    
    # Step 4: Store processed data in the database
    logger.info("Storing processed data in the database")
    success = store_dataframe(processed_data)
    
    if success:
        logger.info("Pipeline completed successfully")
    else:
        logger.error("Failed to store data in the database")

def main():
    """Main function to parse arguments and run the pipeline"""
    parser = argparse.ArgumentParser(description='Run the data pipeline')
    
    # SFTP configuration
    parser.add_argument('--host', default=os.getenv('SFTP_HOST', 'localhost'),
                        help='SFTP host (default: from SFTP_HOST env var or localhost)')
    parser.add_argument('--port', type=int, default=int(os.getenv('SFTP_PORT', '22')),
                        help='SFTP port (default: from SFTP_PORT env var or 22)')
    parser.add_argument('--username', default=os.getenv('SFTP_USERNAME', 'user'),
                        help='SFTP username (default: from SFTP_USERNAME env var or user)')
    parser.add_argument('--password', default=os.getenv('SFTP_PASSWORD', 'password'),
                        help='SFTP password (default: from SFTP_PASSWORD env var or password)')
    parser.add_argument('--remote-dir', default=os.getenv('SFTP_REMOTE_DIR', '/data'),
                        help='SFTP remote directory (default: from SFTP_REMOTE_DIR env var or /data)')
    parser.add_argument('--local-dir', default=os.getenv('LOCAL_DIR', './downloaded_data'),
                        help='Local directory for downloaded files (default: from LOCAL_DIR env var or ./downloaded_data)')
    
    args = parser.parse_args()
    
    # Create SFTP configuration
    sftp_config = {
        'host': args.host,
        'port': args.port,
        'username': args.username,
        'password': args.password,
        'remote_dir': args.remote_dir,
        'local_dir': args.local_dir
    }
    
    # Run the pipeline
    run_pipeline(sftp_config)

if __name__ == "__main__":
    main()