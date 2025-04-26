import json
import subprocess
import time
import random
import sys
import boto3
import re
import os

# Ensure required packages are installed
def install_required_packages():
    required_packages = ['anticaptchaofficial', 'boto3', 'requests', 'beautifulsoup4']
    for package in required_packages:
        try:
            # Try to import the package
            __import__(package)
            print(f"Package {package} is already installed.")
        except ImportError:
            # If import fails, install the package
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Successfully installed {package}.")

# Install required packages first
print("Checking required packages...")
install_required_packages()

# Initialize S3 client
s3 = boto3.client('s3')
bucket_name = 'datainsdr'
s3_prefix = 'PNC26-4/'

# Function to run get_session.py and update session ID
def refresh_session():
    print("Refreshing session ID by running get_session.py...")
    subprocess.run([sys.executable, 'get_session.py'], check=True)
    print("Session ID refreshed successfully")

# Check which IDs are already processed in S3
def get_existing_ids():
    existing_ids = set()
    try:
        # List objects in the S3 bucket with the given prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                # Extract the ID from the file name (assuming format is ID.html)
                key = obj['Key']
                if key.endswith('.html'):
                    file_name = key.split('/')[-1]
                    id_num = file_name.split('.')[0]
                    existing_ids.add(id_num)
        
        print(f"Found {len(existing_ids)} already processed IDs in S3")
    except Exception as e:
        print(f"Error checking S3 for existing files: {e}")
    
    return existing_ids

# Function to check if session has expired in the HTML content
def is_session_expired(html_content):
    return '<div class ="please-note">' in html_content and 'I agree to the Terms of Use' in html_content

# Function to check local file content for session expiration
def check_file_for_expiration(file_path):
    if not os.path.exists(file_path):
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            return is_session_expired(content)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return False

# Load IDs from both JSON files
with open('26-4.json', 'r') as f:
    data = json.load(f)
    id_list_26_4 = data.get('extracted_values', [])

with open('ALL.json', 'r') as f:
    data = json.load(f)
    id_list_all = [str(id) for id in data.get('ALL', [])]  # Convert integers to strings

# Combine both lists while maintaining order (26-4 first, then ALL)
id_list = id_list_26_4 + id_list_all

# Get list of already processed IDs
processed_ids = get_existing_ids()

# Process each ID
for id_num in id_list:
    # Skip if already processed
    if id_num in processed_ids:
        print(f"Skipping ID {id_num} - already processed")
        continue
        
    print(f"\n===== Processing ID: {id_num} =====")
    
    # Try processing with retry logic
    max_retries = 3
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        try:
            # Call Maybe.py with the ID as command line argument
            result = subprocess.run([sys.executable, 'Maybe.py', id_num], capture_output=True, text=True)
            
            # Local file path
            local_file = f"{id_num}.html"
            
            # Check if the output indicates session expiration
            if is_session_expired(result.stdout) or check_file_for_expiration(local_file):
                print("Session expired. Refreshing session ID...")
                refresh_session()
                continue  # Retry without incrementing retry count
            
            # Check if the local file exists and has valid content before uploading
            if not os.path.exists(local_file):
                print(f"File {local_file} not found. Retrying...")
                retry_count += 1
                continue
                
            # Upload the generated HTML file to S3
            s3_key = f"{s3_prefix}{id_num}.html"
            
            s3.upload_file(local_file, bucket_name, s3_key)
            print(f"Successfully processed ID: {id_num}, output saved to S3 as {s3_key}")
            success = True
        
        except Exception as e:
            retry_count += 1
            if "Max retries exceeded" in str(e) or "Tunnel connection failed" in str(e) or "522 status code" in str(e) or "ProxyError" in str(e):
                retry_delay = random.uniform(5, 10)
                print(f"Connection error for ID {id_num}: {e}")
                print(f"Retrying in {retry_delay:.2f} seconds... (Attempt {retry_count}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"Error processing ID {id_num}: {e}")
                if retry_count < max_retries:
                    retry_delay = random.uniform(5, 10)
                    print(f"Retrying in {retry_delay:.2f} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    print(f"Max retries reached for ID {id_num}, moving to next ID")
                    break
    
    # Random delay between IDs
    delay = random.uniform(5, 10)
    print(f"Waiting {delay:.2f} seconds before next ID...")
    time.sleep(delay)

print("All IDs have been processed.")
