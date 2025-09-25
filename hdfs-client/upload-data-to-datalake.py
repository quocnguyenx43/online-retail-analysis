#!/usr/bin/env python3

import os
import sys
import time
from hdfs import InsecureClient
from hdfs.util import HdfsError

# HDFS config
HDFS_URL = "http://namenode:9870" 
HDFS_USER = "hdfs-client"
HDFS_DIR = "/datalake"

# Local raw data directory
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/tmp/data/raw")

# Retry configuration
MAX_RETRIES = 5   # seconds
RETRY_DELAY = 10  # seconds

def wait_for_hdfs():
    """Wait for HDFS namenode to be available."""
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Attempting to connect to HDFS... (attempt {attempt + 1}/{MAX_RETRIES})")
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            # Test connection by listing root directory
            client.list('/')
            print("Successfully connected to HDFS")
            return client
        except Exception as e:
            print(f"Connection failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached. Exiting.")
                sys.exit(1)

def ensure_hdfs_dir(client, path: str):
    """Create folder in HDFS if it does not exist."""
    try:
        if not client.status(path, strict=False):
            print(f"Creating HDFS folder: {path}")
            client.makedirs(path)
        else:
            print(f"Folder already exists: {path}")
    except HdfsError as e:
        print(f"Error creating/checking HDFS directory {path}: {e}")
        sys.exit(1)

def upload_csvs(client, local_dir: str, hdfs_dir: str):
    """Upload all CSVs from local_dir to hdfs_dir."""
    if not os.path.isdir(local_dir):
        print(f"Local folder not found: {local_dir}")
        return

    csv_files = [f for f in os.listdir(local_dir) if f.endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in {local_dir}")
        return

    for fname in csv_files:
        local_path = os.path.join(local_dir, fname)
        hdfs_path = f"{hdfs_dir}/{fname}"
        try:
            print(f"Uploading {local_path} → {hdfs_path} successfully")
            client.upload(hdfs_path, local_path, overwrite=True)
            print(f"Successfully uploaded {fname}")
        except Exception as e:
            print(f"Error uploading {fname}: {e}")
            sys.exit(1)

def main():
    print("Starting HDFS data upload process...")
    
    # Wait for HDFS to be available
    client = wait_for_hdfs()
    
    # Ensure target directory exists
    ensure_hdfs_dir(client, HDFS_DIR)
    
    # Upload CSV files
    upload_csvs(client, RAW_DATA_DIR, HDFS_DIR)
    
    print("Data upload completed successfully. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    main()
