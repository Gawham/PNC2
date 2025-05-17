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
s3_prefix = 'PNC17May/'

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

# Load IDs from both JSON files
with open('May17.json', 'r') as f:
    data = json.load(f)
    id_list = data.get('extracted_values', [])

# Get list of already processed IDs
processed_ids = get_existing_ids()

# Print total IDs to be processed
total_ids = len(id_list)
already_processed = len(processed_ids)
to_be_processed = total_ids - len(processed_ids.intersection(id_list))
print(f"\nTotal IDs in May17.json: {total_ids}")
print(f"Already processed: {already_processed}")
print(f"Remaining to be processed: {to_be_processed}\n")

# Process each ID
consecutive_non_proxy_errors = 0

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
            result = subprocess.run([sys.executable, 'Maybe.py', id_num, 'xvvhnbnvz4ecp51i4lksmsms'], capture_output=True, text=True)
            
            # Print stdout and stderr from Maybe.py
            if result.stdout:
                print("Maybe.py output:", result.stdout)
            if result.stderr:
                print("Maybe.py errors:", result.stderr)
            
            # Local file path
            local_file = f"{id_num}.html"
            
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
            consecutive_non_proxy_errors = 0
        
        except Exception as e:
            retry_count += 1
            if "Max retries exceeded" in str(e) or "Tunnel connection failed" in str(e) or "522 status code" in str(e) or "ProxyError" in str(e):
                print(f"Proxy timeout for ID {id_num}, retrying immediately... (Attempt {retry_count}/{max_retries})")
                consecutive_non_proxy_errors = 0
                continue
            else:
                print(f"Error processing ID {id_num}: {e}")
                consecutive_non_proxy_errors += 1
                if consecutive_non_proxy_errors >= 5:
                    print("5 consecutive non-proxy errors detected. Ending process.")
                    sys.exit(1)
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
