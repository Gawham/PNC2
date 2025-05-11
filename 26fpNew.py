import csv
import json
import requests
import time
import os
import boto3
import random
from requests.exceptions import Timeout
from botocore.exceptions import ClientError

def check_file_exists_in_s3(bucket_name, s3_path):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_path)
        return True
    except Exception:
        return False

def get_brightdata_api_key():
    secret_name = "brightdata/api-key"
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return get_secret_value_response['SecretString']
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

def make_api_call(name, city, state, notice_id, max_retries=25):
    formatted_name = name.lower().replace(' ', '-')
    formatted_city = city.lower().replace(' ', '-')
    formatted_state = state.lower()
    
    output_filename = f"{notice_id}_{formatted_name}_{formatted_city}_{formatted_state}.html"
    bucket_name = "datainsdr"
    s3_path = f"PNCFP3/{output_filename}"
    
    if check_file_exists_in_s3(bucket_name, s3_path):
        print(f"Response file already exists for {name} in {city}, {state} in S3, skipping...")
        return "already_exists"

    url = f"https://www.fastpeoplesearch.com/name/{formatted_name}_{formatted_city}-{formatted_state}"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {get_brightdata_api_key()}'
    }
    
    data = {
        "zone": "web_unlocker1",
        "url": url,
        "format": "raw"
    }

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            print(f"Attempt {attempt} for {name} in {city}, {state}")
            response = requests.post('https://api.brightdata.com/request', 
                                   headers=headers,
                                   json=data,
                                   timeout=15)
            
            if response.status_code == 200 and response.text:
                print("Received valid content, uploading to S3")
                
                s3_client = boto3.client('s3')
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_path,
                    Body=response.text.encode('utf-8')
                )
                print(f"Uploaded to S3: {s3_path}")
                return response.text
            else:
                print(f"Error response: Status {response.status_code}")
                print(response.text)
                sleep_time = min(300, 2 ** attempt + random.uniform(0, 1))
                print(f"Backing off for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        except Timeout:
            print(f"Request timed out on attempt {attempt}, retrying...")
            sleep_time = min(300, 2 ** attempt + random.uniform(0, 1))
            print(f"Backing off for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
        except Exception as e:
            print(f"Error on attempt {attempt}: {str(e)}")
            sleep_time = min(300, 2 ** attempt + random.uniform(0, 1))
            print(f"Backing off for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
    
    print(f"Failed to get valid response after {max_retries} attempts")
    return None

def process_csv_file():
    input_csv = "processed_notices_cleaned.csv"
    bucket_name = "datainsdr"
    
    print(f"Looking for existing files in S3 bucket '{bucket_name}' under prefix 'PNCFP3/'")
    # Get list of all existing files in S3 first
    existing_files = set()
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix="PNCFP3/"):
        if 'Contents' in page:
            for obj in page['Contents']:
                existing_files.add(obj['Key'].replace('PNCFP3/', ''))
    
    print(f"Found {len(existing_files)} existing files in '{bucket_name}/PNCFP3/'")
    
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
                output_filename = f"{notice_id}_{deceased_name.lower().replace(' ', '-')}_{deceased_city.lower().replace(' ', '-')}_{deceased_state}.html"
                if output_filename not in existing_files:
                    pending_items_by_notice_id[notice_id].append((deceased_name, deceased_city, deceased_state))
            
            # Then try representative name
            rep_name = row.get('representative_name', '').strip()
            rep_city = row.get('representative_city', '').strip()
            rep_state = row.get('representative_state', 'CO').strip().lower()
            
            # If representative name is valid, add to pending items
            if rep_name and rep_city:
                output_filename = f"{notice_id}_{rep_name.lower().replace(' ', '-')}_{rep_city.lower().replace(' ', '-')}_{rep_state}.html"
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
            if not result or result == "already_exists":
                continue

def main():
    process_csv_file()

if __name__ == "__main__":
    main()