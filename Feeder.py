import json
import subprocess
import time
import random
import sys
import boto3
import tempfile
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

# Load IDs from JSON file
with open('26-4.json', 'r') as f:
    data = json.load(f)
    id_list = data.get('extracted_values', [])

# Get list of already processed IDs
processed_ids = get_existing_ids()

# Process each ID
for id_num in id_list:
    # Skip if already processed
    if id_num in processed_ids:
        print(f"Skipping ID {id_num} - already processed")
        continue
    
    # Set up retry mechanism
    max_retries = 3
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        if retry_count > 0:
            print(f"\n===== Retrying ID: {id_num} (Attempt {retry_count+1}/{max_retries}) =====")
            # Wait 5 seconds before retrying
            print("Waiting 5 seconds before retry...")
            time.sleep(5)
        else:
            print(f"\n===== Processing ID: {id_num} =====")
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as temp_file:
            temp_file_path = temp_file.name
        
        try:
            # Call Maybe.py with the ID as command line argument and capture output
            print(f"Running Maybe.py for ID {id_num}...")
            process = subprocess.run(
                [sys.executable, 'Maybe.py', id_num, temp_file_path], 
                check=False,
                capture_output=True,
                text=True
            )
            
            # Log the output from Maybe.py
            print("\n--- Maybe.py STDOUT ---")
            print(process.stdout)
            print("\n--- Maybe.py STDERR ---")
            print(process.stderr)
            print("--- End Maybe.py output ---\n")
            
            # Check if process was successful
            if process.returncode != 0:
                # Check if error is related to proxy
                if "ProxyError" in process.stderr or "Tunnel connection failed" in process.stderr:
                    retry_count += 1
                    print(f"Proxy error detected, will retry. Attempt {retry_count}/{max_retries}")
                    continue  # Skip the rest of this iteration and retry
                else:
                    raise Exception(f"Maybe.py exited with code {process.returncode}")
            
            # Upload the generated HTML file to S3
            if os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0:
                s3_key = f"{s3_prefix}{id_num}.html"
                s3.upload_file(temp_file_path, bucket_name, s3_key)
                print(f"Successfully processed ID: {id_num}, saved directly to S3 as {s3_key}")
                success = True  # Mark as successful
            else:
                raise Exception("Output file was not created or is empty")
            
        except Exception as e:
            print(f"Error processing ID {id_num}: {e}")
            if "ProxyError" in str(e) or "Tunnel connection failed" in str(e):
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Proxy error detected, will retry. Attempt {retry_count}/{max_retries}")
                    continue  # Skip the rest of this iteration and retry
            else:
                # For non-proxy errors, don't retry
                break
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    
    # Random delay between 5-10 seconds before the next ID
    if not (retry_count == max_retries and not success):  # Skip delay if we just did retries and failed
        delay = random.uniform(5, 10)
        print(f"Waiting {delay:.2f} seconds before next ID...")
        time.sleep(delay)

print("All IDs have been processed.")
