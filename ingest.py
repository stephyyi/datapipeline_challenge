"""
SFTP Data Ingestion Module
"""
import os
import paramiko
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_sftp(host, port, username, password):
    """Establish connection to SFTP server"""
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("Successfully connected to SFTP server")
        return sftp
    except Exception as e:
        logging.error(f"Failed to connect to SFTP server: {str(e)}")
        # we would send alerts here
        return None


def download_files(sftp, remote_dir, local_dir):
    """Download files from SFTP server to local directory"""
    try:
        os.makedirs(local_dir, exist_ok=True)
        
        # List files in remote directory
        files = sftp.listdir(remote_dir)
        downloaded_files = []
        
        for filename in files:
            if filename.endswith('.csv') or filename.endswith('.json'):
                remote_path = f"{remote_dir}/{filename}"
                local_path = f"{local_dir}/{filename}"
                
                # Download file
                sftp.get(remote_path, local_path)
                logging.info(f"Downloaded {filename}")
                downloaded_files.append(local_path)
        
        return downloaded_files
    except Exception as e:
        logging.error(f"Error downloading files: {str(e)}")
        return []


def ingest_data(config):
    """Main function to ingest data from SFTP"""
    sftp = connect_sftp(
        config['host'],
        config['port'],
        config['username'],
        config['password']
    )

    if sftp:
        try:
            files = download_files(sftp, config['remote_dir'], config['local_dir'])
            sftp.close()
            return files
        except Exception as e:
            logging.error(f"Error in data ingestion: {str(e)}")
    
            if sftp:
                sftp.close()

    return []


# For testing
if __name__ == "__main__":
    config = {
        'host': 'localhost',
        'port': 22,
        'username': 'user',
        'password': 'password123',
        'remote_dir': '/data',
        'local_dir': './downloaded_data'
    }

    files = ingest_data(config)
    print(f"Downloaded files: {files}")
