import csv
import subprocess
import json
import time
import os
import boto3
import tempfile
import sys
import urllib.request
import ssl
import gzip
import io
from urllib.parse import urlparse
import asyncio
import aiohttp
import math
import glob

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

def get_proxy():
    secret_name = "proxy/credentials"
    region_name = "ap-south-1"
    
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
        return secret['proxy_url']
    except Exception as e:
        print(f"Error retrieving proxy: {e}")
        raise

async def make_api_call_async(session, name, city, state, notice_id, max_retries=10):
    # First replace spaces with hyphens
    formatted_name = name.lower().replace(' ', '-')
    
    # Then replace any original hyphens with tildes
    if '-' in name:  # Only if the original name had hyphens
        # Split by space first to avoid replacing the spaces-turned-hyphens
        name_parts = name.split(' ')
        formatted_parts = []
        for part in name_parts:
            if '-' in part:
                formatted_parts.append(part.lower().replace('-', '~'))
            else:
                formatted_parts.append(part.lower())
        formatted_name = '-'.join(formatted_parts)
    
    # Format the city and state
    formatted_city = city.lower().replace(' ', '-')
    formatted_state = state.lower()
    
    # Check if response file already exists in S3
    formatted_output_name = name.replace(' ', '_').replace('-', '_')
    formatted_city_name = city.lower().replace(' ', '-')  # Use hyphens for city
    output_filename = f"{notice_id}_{formatted_output_name}_{formatted_city_name}_{formatted_state}.html"
    bucket_name = "datainsdr"
    s3_path = f"PNCBigBoy/{output_filename}"
    
    if check_file_exists_in_s3(bucket_name, s3_path):
        print(f"Response file already exists for {notice_id}_{name} in S3, skipping...")
        return "already_exists"
    
    # Construct the URL with city and state
    url = f"https://www.fastpeoplesearch.com/name/{formatted_name}_{formatted_city}-{formatted_state}"
    print(f"Using URL: {url}")
    
    # Get proxy from Secrets Manager
    proxy = get_proxy()
    
    # Keep trying until successful or max retries reached
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            print(f"Attempt {attempt} for {notice_id}_{name} in {city}, {state}")
            
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.get(url, proxy=proxy, ssl=False, timeout=timeout) as response:
                response_text = await response.text()
            
            # Check if we got a valid response
            if response_text and len(response_text.strip()) > 0:
                print("Received valid content, uploading to S3")
                
                # Upload HTML directly to S3
                s3_client = boto3.client('s3')
                s3_client.put_object(
                    Body=response_text,
                    Bucket=bucket_name,
                    Key=s3_path,
                    ContentType='text/html'
                )
                
                # Also save locally
                os.makedirs('responses', exist_ok=True)
                local_path = os.path.join('responses', output_filename)
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(response_text)
                print(f"Saved response to {local_path}")
                
                return response_text
            else:
                print("Empty content in response, retrying...")
        except asyncio.TimeoutError:
            print(f"Timeout on attempt {attempt} after 60 seconds")
        except Exception as e:
            print(f"Error on attempt {attempt}: {str(e)}")
        await asyncio.sleep(2)  # Add a small delay between retries
    
    print(f"Failed to get valid response after {max_retries} attempts")
    return None

async def process_csv_file_async(csv_file_path):
    bucket_name = "datainsdr"
    
    # Get list of all existing files in S3 first
    s3_client = boto3.client('s3')
    existing_files = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    print(f"\nChecking existing files in s3://{bucket_name}/PNCBigBoy/")
    for page in paginator.paginate(Bucket=bucket_name, Prefix="PNCBigBoy/"):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('/'):
                    filename = os.path.basename(key)
                    existing_files.add(filename)
    
    print(f"\nFound {len(existing_files)} existing files in S3")
    
    if not os.path.exists(csv_file_path):
        print(f"Error: Input file not found: {csv_file_path}")
        return
    
    # Collect all items and check which ones are missing
    all_items = []
    missing_items = []
    processed_notice_ids = set()
    
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            notice_id = row.get('notice_id', '').strip()
            if not notice_id:
                continue
            
            processed_notice_ids.add(notice_id)
            
            # Process deceased person
            deceased_name = row.get('deceased_name', '').strip()
            deceased_city = row.get('deceased_city', '').strip()
            deceased_state = row.get('deceased_state', 'CO').strip()
            
            if deceased_name and deceased_city:
                # Format name with underscores, but city with hyphens
                formatted_name = deceased_name.replace(' ', '_').replace('-', '_')
                formatted_city = deceased_city.lower().replace(' ', '-')  # Use hyphens for city
                formatted_state = deceased_state.lower()
                output_filename = f"{notice_id}_{formatted_name}_{formatted_city}_{formatted_state}.html"
                item = (deceased_name, deceased_city, deceased_state, notice_id, "deceased")
                all_items.append((item, output_filename))
            
            # Process representative
            rep_name = row.get('representative_name', '').strip()
            rep_city = row.get('representative_city', '').strip()
            rep_state = row.get('representative_state', 'CO').strip()
            
            if rep_name and rep_city:
                # Format name with underscores, but city with hyphens
                formatted_name = rep_name.replace(' ', '_').replace('-', '_')
                formatted_city = rep_city.lower().replace(' ', '-')  # Use hyphens for city
                formatted_state = rep_state.lower()
                output_filename = f"{notice_id}_{formatted_name}_{formatted_city}_{formatted_state}.html"
                item = (rep_name, rep_city, rep_state, notice_id, "representative")
                all_items.append((item, output_filename))
    
    # Count unique notice IDs in S3
    s3_notice_ids = set()
    for filename in existing_files:
        notice_id = filename.split('_')[0]
        s3_notice_ids.add(notice_id)
    
    # Check which items are missing
    missing_by_type = {"deceased": 0, "representative": 0}
    found_by_type = {"deceased": 0, "representative": 0}
    
    for item, filename in all_items:
        name, city, state, notice_id, person_type = item
        if filename in existing_files:
            found_by_type[person_type] += 1
        else:
            missing_by_type[person_type] += 1
            missing_items.append((name, city, state, notice_id))
    
    print("\nDETAILED STATS:")
    print(f"Total unique notice IDs in CSV: {len(processed_notice_ids)}")
    print(f"Total unique notice IDs in S3: {len(s3_notice_ids)}")
    print(f"Total entries in CSV: {len(all_items)}")
    print(f"- Deceased entries: {found_by_type['deceased'] + missing_by_type['deceased']}")
    print(f"  - Found in S3: {found_by_type['deceased']}")
    print(f"  - Missing: {missing_by_type['deceased']}")
    print(f"- Representative entries: {found_by_type['representative'] + missing_by_type['representative']}")
    print(f"  - Found in S3: {found_by_type['representative']}")
    print(f"  - Missing: {missing_by_type['representative']}")
    print(f"\nTotal files in S3: {len(existing_files)}")
    print(f"Total missing entries: {len(missing_items)}")
    
    # Print some example filenames for verification
    print("\nExample filenames in S3:")
    for filename in list(existing_files)[:5]:
        print(f"- {filename}")
    
    print("\nExample filenames we're looking for:")
    for _, filename in all_items[:5]:
        print(f"- {filename}")
    
    if len(missing_items) > 0:
        print("\nMissing entries:")
        for name, city, state, notice_id in missing_items:
            print(f"- Notice ID: {notice_id}, Name: {name}, Location: {city}, {state}")
    
    print("=" * 50)
    
    if len(missing_items) == 0:
        print("No new items to process")
        return
    
    proceed = input("\nDo you want to proceed with processing these entries? (y/n): ")
    if proceed.lower() != 'y':
        print("Aborting processing")
        return
    
    failed_entries = []
    processed_count = 0
    
    print("\nStarting processing of all missing entries...")
    async with aiohttp.ClientSession() as session:
        tasks = [make_api_call_async(session, name, city, state, notice_id) for name, city, state, notice_id in missing_items]
        results = await asyncio.gather(*tasks)
        
        # Track failed entries and count successes
        for item, result in zip(missing_items, results):
            if not result or result == "already_exists":
                failed_entries.append(item)
            elif result:
                processed_count += 1
    
    print(f"\nFinal Results:")
    print(f"Successfully processed {processed_count} out of {len(missing_items)} items")
    print(f"Failed entries ({len(failed_entries)}):")
    for name, city, state, notice_id in failed_entries:
        print(f"- Notice ID: {notice_id}, Name: {name}, Location: {city}, {state}")
    print("=" * 50)

def main():
    # Process the processed_notices.csv file
    csv_file = "processed_notices.csv"
    print(f"Processing {csv_file}")
    print("=" * 50)
    asyncio.run(process_csv_file_async(csv_file))
    print("=" * 50)
    print(f"Completed processing {csv_file}")

if __name__ == "__main__":
    main()