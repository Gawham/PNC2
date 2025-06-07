import requests
import os
import csv
import time
import boto3

# Initialize S3 client
s3 = boto3.client('s3')
BUCKET_NAME = 'datainsdr'
BASE_PREFIX = 'PNC_Images'

# Create panorama directory if it doesn't exist (for temporary storage)
if not os.path.exists('panorama'):
    os.makedirs('panorama')

# Read the CSV file
with open('processed_notices_updated.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        if not row['deceased_address'] or not row['notice_id']:  # Skip if address or notice_id is empty
            continue
            
        address = row['deceased_address']
        notice_id = row['notice_id']
        
        # Check if folder has exactly 1 image
        prefix = f"{BASE_PREFIX}/{notice_id}/"
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response or len(response['Contents']) != 1:
            print(f"Skipping notice_id {notice_id} - does not have exactly 1 image")
            continue

        # Geocode the address
        params = {
            'address': address,
            'key': 'AIzaSyDQBekN6vij7yL2Mudx6xuXGVy77GfaP-A',
        }

        try:
            response = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
            data = response.json()
            
            if not data['results']:
                print(f"Could not geocode address: {address}")
                continue
                
            lat = data['results'][0]['geometry']['location']['lat']
            lng = data['results'][0]['geometry']['location']['lng']
            print(f"Processing notice_id: {notice_id}, address: {address}")
            print(f"Latitude: {lat}")
            print(f"Longitude: {lng}")

            # Capture images at 45-degree intervals for a full 360° view
            for heading in range(0, 360, 45):
                streetview_params = {
                    'size': '2048x2048',  # Maximum supported size
                    'fov': '90',          # Standard field of view
                    'heading': str(heading),
                    'pitch': '0',
                    'key': 'AIzaSyDQBekN6vij7yL2Mudx6xuXGVy77GfaP-A',
                    'location': f"{lat},{lng}"
                }

                image_response = requests.get('https://maps.googleapis.com/maps/api/streetview', params=streetview_params)
                
                # Create a temporary local file
                temp_file = f'panorama/streetview_{heading}.jpg'
                with open(temp_file, 'wb') as f:
                    f.write(image_response.content)
                
                # Upload to S3
                s3_key = f"{BASE_PREFIX}/{notice_id}/streetview_{heading}.jpg"
                s3.upload_file(temp_file, BUCKET_NAME, s3_key)
                print(f"Uploaded image at heading {heading}° for notice_id {notice_id} to S3")
                
                # Remove temporary file
                os.remove(temp_file)
                
            # Add a small delay to avoid hitting rate limits
            time.sleep(1)
            
        except Exception as e:
            print(f"Error processing notice_id {notice_id}, address {address}: {str(e)}")
            continue

# Clean up temporary directory
os.rmdir('panorama')