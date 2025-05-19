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

async def make_api_call_async(session, name, city, state, notice_id, max_retries=3):
    # Format the name for the URL (lowercase, replace spaces with hyphens)
    formatted_name = name.lower().replace(' ', '-')
    # Replace any existing hyphens in original name with tildes
    if '-' in name:
        formatted_name = formatted_name.replace('-', '~')
    
    # Format the city and state
    formatted_city = city.lower().replace(' ', '-')
    formatted_state = state.lower()
    
    # Check if response file already exists in S3
    formatted_output_name = name.replace(' ', '_')
    output_filename = f"{notice_id}_{formatted_output_name}_{formatted_city}_{formatted_state}.html"
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
            
            timeout = aiohttp.ClientTimeout(total=45)
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
            print(f"Timeout on attempt {attempt} after 45 seconds")
        except Exception as e:
            print(f"Error on attempt {attempt}: {str(e)}")
        await asyncio.sleep(2)  # Add a small delay between retries
    
    print(f"Failed to get valid response after {max_retries} attempts")
    return None

async def process_batch(batch, session):
    tasks = []
    for name, city, state, notice_id in batch:
        task = asyncio.create_task(make_api_call_async(session, name, city, state, notice_id))
        tasks.append(task)
    return await asyncio.gather(*tasks)

async def process_csv_file_async(csv_file_path):
    bucket_name = "datainsdr"
    failed_entries = []  # Track failed entries
    
    # Get list of all existing files in S3 first
    s3_client = boto3.client('s3')
    existing_files = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    print(f"Checking for files in s3://{bucket_name}/PNCBigBoy/")
    for page in paginator.paginate(Bucket=bucket_name, Prefix="PNCBigBoy/"):
        if 'Contents' in page:
            print(f"Found {len(page['Contents'])} files in this page")
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('/'):
                    existing_files.add(os.path.basename(key))
    
    print(f"Found {len(existing_files)} existing files")
    
    if not os.path.exists(csv_file_path):
        print(f"Error: Input file not found: {csv_file_path}")
        return
    
    # Collect all pending items first
    pending_items = []
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            notice_id = row.get('notice_id', '').strip()
            if not notice_id:
                continue
            
            # Process deceased person
            deceased_name = row.get('deceased_name', '').strip()
            deceased_city = row.get('deceased_city', '').strip()
            deceased_state = row.get('deceased_state', 'CO').strip()
            
            if deceased_name and deceased_city:
                formatted_output_name = deceased_name.replace(' ', '_')
                output_filename = f"{notice_id}_{formatted_output_name}_{deceased_city.lower()}_{deceased_state.lower()}.html"
                if output_filename not in existing_files:
                    pending_items.append((deceased_name, deceased_city, deceased_state, notice_id))
            
            # Process representative
            rep_name = row.get('representative_name', '').strip()
            rep_city = row.get('representative_city', '').strip()
            rep_state = row.get('representative_state', 'CO').strip()
            
            if rep_name and rep_city:
                formatted_output_name = rep_name.replace(' ', '_')
                output_filename = f"{notice_id}_{formatted_output_name}_{rep_city.lower()}_{rep_state.lower()}.html"
                if output_filename not in existing_files:
                    pending_items.append((rep_name, rep_city, rep_state, notice_id))
    
    total_pending = len(pending_items)
    print(f"\nSTATS:")
    print(f"Total entries to process: {total_pending}")
    print(f"Already processed: {len(existing_files)}")
    print(f"Remaining: {total_pending}")
    print("=" * 50)
    
    if total_pending == 0:
        print("No new items to process")
        return
    
    # Process in batches of 50
    batch_size = 50
    num_batches = math.ceil(total_pending / batch_size)
    processed_count = 0
    remaining = total_pending
    
    async with aiohttp.ClientSession() as session:
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_pending)
            current_batch = pending_items[start_idx:end_idx]
            
            print(f"\nProcessing batch {i+1}/{num_batches} ({len(current_batch)} items)")
            results = await process_batch(current_batch, session)
            
            # Track failed entries
            for item, result in zip(current_batch, results):
                if not result or result == "already_exists":
                    failed_entries.append(item)
            
            # Count successful results
            successful = sum(1 for r in results if r and r != "already_exists")
            processed_count += successful
            remaining = total_pending - processed_count
            
            print(f"\nBatch {i+1} Complete:")
            print(f"Successfully processed in this batch: {successful}")
            print(f"Total processed so far: {processed_count}")
            print(f"Remaining items: {remaining}")
            print("-" * 50)
            
            # Small delay between batches
            if i < num_batches - 1:
                await asyncio.sleep(5)
    
    print(f"\nFinal Results:")
    print(f"Successfully processed {processed_count} out of {total_pending} items")
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