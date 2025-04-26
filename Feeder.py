import json
import subprocess
import time
import random
import sys
import boto3

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
        
    print(f"\n===== Processing ID: {id_num} =====")
    
    # Run Maybe.py with the current ID
    try:
        # Call Maybe.py with the ID as command line argument
        subprocess.run([sys.executable, 'Maybe.py', id_num], check=True)
        
        # Upload the generated HTML file to S3
        local_file = f"{id_num}.html"
        s3_key = f"{s3_prefix}{id_num}.html"
        
        s3.upload_file(local_file, bucket_name, s3_key)
        print(f"Successfully processed ID: {id_num}, output saved to S3 as {s3_key}")
    
    except Exception as e:
        print(f"Error processing ID {id_num}: {e}")
    
    # Random delay between 5-10 seconds before next ID
    delay = random.uniform(5, 10)
    print(f"Waiting {delay:.2f} seconds before next ID...")
    time.sleep(delay)

print("All IDs have been processed.")
