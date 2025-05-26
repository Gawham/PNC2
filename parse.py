import json
from bs4 import BeautifulSoup
import re
import boto3
import os
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

def clean_text(text):
    # Remove hidden text and UUIDs
    text = re.sub(r'<span style="display:none;">.*?</span>', '', text)
    text = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}Commercial use of this website is strictly prohibited\.Contact the administrator with any questions\.[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', '', text)
    
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Replace "\n" followed by spaces with just "\n"
    text = re.sub(r'\n\s+', '\n', text)
    
    # Replace multiple newlines with single newline
    text = re.sub(r'\n+', '\n', text)
    
    # Remove spaces before punctuation
    text = re.sub(r'\s+([.,])', r'\1', text)
    
    # Strip whitespace
    text = text.strip()
    
    return text

def parse_notice_html(html_content, notice_id, filename):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Get notice text
    notice_text_element = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_PublicNoticeDetailsBody1_lblContentText'})
    notice_text = notice_text_element.get_text() if notice_text_element else ""
    
    # Clean and format the text
    notice_text = clean_text(notice_text)
    
    # Skip if notice text is empty
    if not notice_text.strip():
        return None
    
    # Get published date
    pub_date_element = soup.find('span', {'id': 'ctl00_ContentPlaceHolder1_PublicNoticeDetailsBody1_lblPublicationDAte'})
    published_date = pub_date_element.get_text() if pub_date_element else ""
    
    # Get URL
    url = f"https://www.publicnoticecolorado.com/Details.aspx?ID={notice_id}"
    
    return {
        notice_id: {
            "url": url,
            "notice_text": notice_text,
            "published_date": published_date,
            "filename": filename,
            "status": "success"
        }
    }

def process_s3_file(s3_client, bucket, key):
    try:
        # Extract numeric ID from filename - handle various formats including just numbers
        id_match = re.search(r'(?:notice[_-]?|diagnostic_notice_|Final_)?(\d+)\.html$', key, re.IGNORECASE)
        if not id_match:
            print(f"Skipping {key} - could not extract notice ID")
            return None
        notice_id = id_match.group(1)
        print(f"Processing {key}...")
        
        # Read file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        html_content = response['Body'].read().decode('utf-8')
        
        # Parse HTML
        result = parse_notice_html(html_content, notice_id, key)
        if result:
            print(f"Successfully processed {key}")
        else:
            print(f"Skipped {key} (empty content)")
        return result
    except Exception as e:
        print(f"Error processing {key}: {str(e)}")
        return None

def should_replace_notice(existing_notice, new_notice):
    """
    Determine if we should replace an existing notice with a new one.
    Returns True if we should use the new notice instead of the existing one.
    """
    # Both notices should have content at this point
    # Keep the existing one by default
    return False

def main():
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Define buckets and prefixes
    s3_locations = [
        ('datainsdr', 'PNC25thMay/'),
    ]
    
    all_results = {}
    files_processed = 0
    files_skipped = 0
    duplicates_found = 0
    total_files_found = 0
    files_by_prefix = {}
    entries_by_prefix = {}  # Track successful entries by prefix
    
    # Process each S3 location
    for bucket, prefix in s3_locations:
        # List all HTML files in the prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        html_files = []
        files_by_prefix[prefix] = 0
        entries_by_prefix[prefix] = 0  # Initialize counter for this prefix
        
        print(f"\nListing files in s3://{bucket}/{prefix}")
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                if obj['Key'].endswith('.html'):
                    html_files.append(obj['Key'])
                    files_by_prefix[prefix] += 1
        
        total_files_found += len(html_files)
        print(f"Found {len(html_files)} HTML files in {prefix}")
        
        # Process files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for key in html_files:
                futures.append(
                    executor.submit(process_s3_file, s3_client, bucket, key)
                )
            
            # Collect results
            for future in futures:
                result = future.result()
                if result:
                    for notice_id, notice_data in result.items():
                        if notice_id in all_results:
                            duplicates_found += 1
                            print(f"Keeping existing notice {notice_id} from {all_results[notice_id]['filename']}")
                        else:
                            all_results[notice_id] = notice_data
                            files_processed += 1
                            # Track which prefix this entry came from
                            if notice_data['filename'].startswith(prefix):
                                entries_by_prefix[prefix] += 1
                else:
                    files_skipped += 1
    
    print("\n=== Summary ===")
    print(f"Total HTML files found: {total_files_found}")
    for prefix, count in files_by_prefix.items():
        print(f"Files in {prefix}: {count}")
        print(f"Successful entries from {prefix}: {entries_by_prefix[prefix]}")
    print(f"Successfully processed: {files_processed}")
    print(f"Skipped (empty/invalid): {files_skipped}")
    print(f"Duplicates found: {duplicates_found}")
    print(f"Total entries in output: {len(all_results)}")
    
    # Save results locally
    output_file = os.path.join(os.getcwd(), 'all_notices.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=4)
    
    print(f"\nResults saved locally to: {output_file}")

if __name__ == "__main__":
    main()
