import requests
import json
import sys
import time
import random
import os
import boto3

# Proxy configuration
proxy_username = 'guhan_U1Lt4'
proxy_password = 'Ishita_23456'
proxies = {
    'http': f'http://{proxy_username}:{proxy_password}@pr.oxylabs.io:7777',
    'https': f'http://{proxy_username}:{proxy_password}@pr.oxylabs.io:7777'
}

# Load cookies from cookies.json
try:
    with open('cookies.json', 'r') as f:
        cookies_list = json.load(f)
        # Convert the list of cookies to a dictionary
        cookies = {}
        for cookie in cookies_list:
            cookies[cookie['name']] = cookie['value']
except FileNotFoundError:
    print("Error: cookies.json not found. Please create the file with the necessary cookies.")
    exit()
except json.JSONDecodeError:
    print("Error: cookies.json is not valid JSON.")
    exit()

# Load IDs from 3May.json
try:
    with open('3May.json', 'r') as f:
        data = json.load(f)
        ids = data.get('extracted_values', [])
except FileNotFoundError:
    print("Error: 3May.json not found.")
    exit()
except json.JSONDecodeError:
    print("Error: 3May.json is not valid JSON.")
    exit()

# S3 configuration
s3_bucket = 'datainsdr'
s3_prefix = 'PNC3May'
s3_client = boto3.client('s3')

# Create local temp directory for files before uploading to S3
temp_dir = 'temp_pnc'
os.makedirs(temp_dir, exist_ok=True)

# Get ID from command line or process all IDs
if len(sys.argv) > 1:
    # Process single ID provided as command line argument
    ids_to_process = [sys.argv[1]]
else:
    # Process all IDs
    ids_to_process = ids

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'dnt': '1',
    'priority': 'u=0, i',
    'referer': 'https://www.publicnoticecolorado.com/(S(cetmjcnkgrm5ppi3ikjml1jn))/Details.aspx?SID=q43xf0daobditvpcak2gcbsf&ID=88405',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 CrKey/1.54.248666',
}

for idx, id_to_use in enumerate(ids_to_process):
    notice_id = id_to_use
    temp_file = os.path.join(temp_dir, f"{notice_id}.html")
    s3_key = f"{s3_prefix}/{notice_id}.html"
    
    # Check if file already exists in S3
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        print(f"Notice {notice_id} already exists in S3. Skipping.")
        continue
    except:
        # File doesn't exist in S3, proceed with download
        pass
    
    params = {
        'SID': 'uwmkhvrgvy45imkvsf43fd3m',
        'ID': id_to_use,
    }

    max_retries = 5
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        try:
            response = requests.get(
                'https://www.publicnoticecolorado.com/(S(uwmkhvrgvy45imkvsf43fd3m))/Details.aspx',
                params=params,
                cookies=cookies,
                headers=headers,
                proxies=proxies,
                timeout=30,
            )
            response.raise_for_status()
            success = True
            
            # Save response content to local temp file
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # Upload to S3
            s3_client.upload_file(temp_file, s3_bucket, s3_key)
            
            # Remove local temp file after upload
            os.remove(temp_file)
            
            print(f"Saved notice {notice_id} to s3://{s3_bucket}/{s3_key}")
            
        except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            retry_count += 1
            print(f"Error fetching notice {notice_id}: {str(e)}")
            if retry_count < max_retries:
                wait_time = 5
                print(f"Retrying in {wait_time} seconds... (Attempt {retry_count+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch notice {notice_id} after {max_retries} attempts.")