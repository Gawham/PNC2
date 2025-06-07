import requests
import boto3
import os
import csv

# Initialize S3 client
s3 = boto3.client('s3')
BUCKET_NAME = 'datainsdr'
BASE_PREFIX = 'PNC_Images3'

def save_streetview(address, notice_id):
    # Create URL with parameters
    params = {
        'location': address,
        'size': '2048x2048',
        'fov': '0',
        'key': 'AIzaSyARFMLB1na-BBWf7_R3-5YOQQaHqEJf6RQ',
        'source': 'outdoor'
    }

    try:
        # Get the image
        image_response = requests.get('https://maps.googleapis.com/maps/api/streetview', params=params)
        
        # Create a temporary local file
        temp_file = f'streetview_{notice_id}.jpg'
        with open(temp_file, 'wb') as f:
            f.write(image_response.content)
        
        # Upload to S3
        s3_key = f"{BASE_PREFIX}/streetview_{notice_id}.jpg"
        s3.upload_file(temp_file, BUCKET_NAME, s3_key)
        print(f"Uploaded image for notice_id {notice_id} to S3")
        
        # Remove temporary file
        os.remove(temp_file)
        
    except Exception as e:
        print(f"Error processing notice_id {notice_id}, address {address}: {str(e)}")

# Process all addresses from CSV
if __name__ == "__main__":
    # Read the CSV file
    with open('processed_notices_updated.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if not row['deceased_address'] or not row['notice_id']:  # Skip if address or notice_id is empty
                continue
                
            address = row['deceased_address']
            notice_id = row['notice_id']
            
            print(f"Processing notice_id: {notice_id}, address: {address}")
            save_streetview(address, notice_id) 