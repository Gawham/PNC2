import csv
import subprocess
import json
import time
import os
import boto3
import tempfile
import sys
import random

# Define process_json_file function since the import is failing
def process_json_file(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            
        # Extract the dox name from the filename
        filename = os.path.basename(json_file_path)
        dox_name = filename.replace('response_', '').replace('.json', '').replace('_', ' ')
        
        # Extract necessary information from JSON
        results = data.get('results', [{}])[0]
        content = results.get('content', '')
        
        # This is a placeholder - you'll need to implement proper parsing logic
        # based on the actual structure of your JSON data
        full_name = "N/A"
        current_address = "N/A"
        current_phone = "N/A"
        secondary_phone = "N/A"
        
        return [dox_name, full_name, current_address, current_phone, secondary_phone]
    except Exception as e:
        print(f"Error processing {json_file_path}: {e}")
        return None

def get_secret():
    secret_name = "fastpeoplesearch/credentials2"
    region_name = "ap-south-1"
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret['username'], secret['password']
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

def upload_to_s3(local_file_path, bucket_name, s3_file_name=None):
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    # If no S3 filename is provided, use the local filename
    if s3_file_name is None:
        s3_file_name = os.path.basename(local_file_path)
    
    try:
        # Upload the file
        s3_client.upload_file(local_file_path, bucket_name, s3_file_name)
        print(f"File {local_file_path} uploaded successfully to {bucket_name}/{s3_file_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")

def check_file_exists_in_s3(bucket_name, s3_path):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_path)
        return True
    except Exception:
        return False

def make_api_call(name, city, state, notice_id, max_retries=25):
    # Format the name for the URL (lowercase, replace spaces with hyphens)
    formatted_name = name.lower().replace(' ', '-')
    
    # Format the city for the URL (lowercase, replace spaces with hyphens)
    formatted_city = city.lower().replace(' ', '-')
    
    # Format the state to lowercase
    formatted_state = state.lower()
    
    # Check if response file already exists in S3
    output_filename = f"{notice_id}_{formatted_name}_{formatted_city}_{formatted_state}.json"
    bucket_name = "datainsdr"
    s3_path = f"PNCFP2/{output_filename}"
    
    if check_file_exists_in_s3(bucket_name, s3_path):
        print(f"Response file already exists for {name} in {city}, {state} in S3, skipping...")
        return "already_exists"
    
    # Construct the URL with state
    url = f"https://www.fastpeoplesearch.com/name/{formatted_name}_{formatted_city}-{formatted_state}"
    print(f"Using URL: {url}")
    
    # Get credentials from AWS Secrets Manager
    username, password = get_secret()
    
    # Construct the curl command
    curl_command = [
        'curl', 'https://realtime.oxylabs.io/v1/queries',
        '--user', f"{username}:{password}",
        '-H', 'Content-Type: application/json',
        '-d', json.dumps({
            "source": "universal",
            "url": url,
            "geo_location": "United States"
        })
    ]
    
    # Keep trying until successful or max retries reached
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            print(f"Attempt {attempt} for {name} in {city}, {state}")
            result = subprocess.run(curl_command, capture_output=True, text=True)
            print(f"API call completed with status: {result.returncode}")
            
            if result.returncode != 0:
                print(f"Curl command failed with error: {result.stderr}")
                # Exponential backoff
                sleep_time = min(300, 2 ** attempt + random.uniform(0, 1))  # Cap at 5 minutes
                print(f"Backing off for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                continue
            
            # Check if we got a valid response with content
            try:
                response_data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON response: {e}")
                print(f"Response was: {result.stdout[:200]}...")  # Print first 200 chars
                # Exponential backoff
                sleep_time = min(300, 2 ** attempt + random.uniform(0, 1))
                print(f"Backing off for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                continue
            
            # Check for empty content array which indicates unsuccessful response
            if 'results' in response_data and response_data['results'] and response_data['results'][0]['content'] != "":
                print("Received valid content, uploading to S3")
                
                # Upload directly to S3 without saving locally
                s3_client = boto3.client('s3')
                try:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_path,
                        Body=result.stdout.encode('utf-8')
                    )
                    print(f"Successfully uploaded to {bucket_name}/{s3_path}")
                    return result.stdout
                except Exception as e:
                    print(f"Error uploading to S3: {e}")
                    raise
            else:
                print("Empty content in response, retrying...")
                # Linear backoff for empty content
                sleep_time = 10 + random.uniform(0, 5)
                print(f"Backing off for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        except Exception as e:
            print(f"Error on attempt {attempt}: {str(e)}")
            # Exponential backoff
            sleep_time = min(300, 2 ** attempt + random.uniform(0, 1))
            print(f"Backing off for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
    
    print(f"Failed to get valid response after {max_retries} attempts")
    return None

def download_from_s3(bucket_name, s3_key, local_file_path):
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket_name, s3_key, local_file_path)
        return True
    except Exception as e:
        print(f"Error downloading file: {e}")
        return False

def process_all_json_files():
    bucket_name = "datainsdr"
    s3_prefix = "SalesNav/"
    output_csv = "AllResults.csv"
    
    # Create S3 client
    s3_client = boto3.client('s3')
    
    # List all objects in the SalesNav folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    
    # Prepare CSV writing
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Dox Name", "Full Name", "Current Address", "Current Phone Number", "Secondary Phone Number"])
        
        # Process each JSON file
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                if s3_key.endswith('.json'):
                    print(f"Processing {s3_key}")
                    
                    # Download the JSON file to a temporary location
                    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
                        temp_file_path = temp_file.name
                    
                    if download_from_s3(bucket_name, s3_key, temp_file_path):
                        # Process the JSON file
                        result = process_json_file(temp_file_path)
                        if result:
                            writer.writerow(result)
                        
                        # Clean up the temporary file
                        os.unlink(temp_file_path)
    
    print(f"Results saved to {output_csv}")

def process_csv_file():
    input_csv = "processed_notices.csv"
    bucket_name = "datainsdr"
    s3_key = "processed_notices.csv"
    s3_client = boto3.client('s3')
    
    print(f"Looking for existing files in S3 bucket '{bucket_name}' under prefix 'PNCFP2/'")
    # Get list of all existing files in S3 first
    existing_files = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix="PNCFP2/"):
        if 'Contents' in page:
            for obj in page['Contents']:
                existing_files.add(obj['Key'].replace('PNCFP2/', ''))
    
    print(f"Found {len(existing_files)} existing files in '{bucket_name}/PNCFP2/'")
    
    # Download and process CSV
    print(f"Downloading CSV file from '{bucket_name}/{s3_key}'")
    try:
        s3_client.download_file(bucket_name, s3_key, input_csv)
    except Exception as e:
        print(f"Error downloading CSV from S3: {e}")
        return
    
    if not os.path.exists(input_csv):
        print(f"Error: Input file not found: {input_csv}")
        return
    
    # Count total entries first
    total_entries = sum(1 for line in open(input_csv)) - 1  # Subtract 1 for header
    print(f"Total entries in '{input_csv}': {total_entries}")
    
    # Collect all pending items first, grouped by notice_id
    pending_items_by_notice_id = {}
    with open(input_csv, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            notice_id = row.get('notice_id', '').strip()
            
            # Initialize the notice_id group if not exists
            if notice_id not in pending_items_by_notice_id:
                pending_items_by_notice_id[notice_id] = []
            
            # Try deceased name first
            deceased_name = row.get('deceased_name', '').strip()
            deceased_city = row.get('deceased_city', '').strip()
            deceased_state = row.get('deceased_state', 'CO').strip().lower()
            
            # If deceased name is valid, add to pending items
            if deceased_name and deceased_city:
                output_filename = f"{notice_id}_{deceased_name.lower().replace(' ', '-')}_{deceased_city.lower().replace(' ', '-')}_{deceased_state}.json"
                if output_filename not in existing_files:
                    pending_items_by_notice_id[notice_id].append((deceased_name, deceased_city, deceased_state))
            
            # Then try representative name
            rep_name = row.get('representative_name', '').strip()
            rep_city = row.get('representative_city', '').strip()
            rep_state = row.get('representative_state', 'CO').strip().lower()
            
            # If representative name is valid, add to pending items
            if rep_name and rep_city:
                output_filename = f"{notice_id}_{rep_name.lower().replace(' ', '-')}_{rep_city.lower().replace(' ', '-')}_{rep_state}.json"
                if output_filename not in existing_files:
                    pending_items_by_notice_id[notice_id].append((rep_name, rep_city, rep_state))
    
    # Remove notice_ids with no pending items
    pending_items_by_notice_id = {k: v for k, v in pending_items_by_notice_id.items() if v}
    
    print(f"Found {len(pending_items_by_notice_id)} notice IDs to process")
    
    # Process all pending items, grouped by notice_id
    for notice_id, items in pending_items_by_notice_id.items():
        print(f"Processing notice ID: {notice_id}")
        
        # Process all names for this notice_id before moving to next
        for name, city, state in items:
            print(f"Processing {name} in {city}, {state}")
            
            result = make_api_call(name, city, state, notice_id)
            if result and result != "already_exists":
                output_filename = f"{notice_id}_{name.lower().replace(' ', '-')}_{city.lower().replace(' ', '-')}_{state}.json"
                s3_path = f"PNCFP2/{output_filename}"
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_path,
                    Body=result.encode('utf-8')
                )

def main():
    # Only process the CSV file to make API calls
    process_csv_file()

if __name__ == "__main__":
    main()