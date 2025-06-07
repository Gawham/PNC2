import boto3
import io
import json
from PIL import Image
from google import genai
import time

# Initialize S3 client
s3 = boto3.client('s3')
bucket = 'datainsdr'
prefix = 'PNC_Images2/'

# Get API key from AWS Secrets Manager
def get_secret():
    secret_name = "gemini/api-key"
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
        return get_secret_value_response['SecretString']
    except Exception as e:
        raise e

# Initialize Gemini client with API key from Secrets Manager
client = genai.Client(api_key=get_secret())

def get_processed_images():
    try:
        with open('house_classifications.json', 'r') as f:
            results = json.load(f)
            return {result['image'] for result in results}
    except FileNotFoundError:
        return set()

def process_image(image_key, processed_images):
    if image_key in processed_images:
        print(f"Skipping {image_key}: already processed")
        return
        
    try:
        # Get image from S3
        response = s3.get_object(Bucket=bucket, Key=image_key)
        image_data = response['Body'].read()
        image = Image.open(io.BytesIO(image_data))
        
        # Get AI response with retry logic
        while True:
            try:
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=[image, "tell me if this house is fancy, normal or rundown or no image only repond with these 4 words"]
                )
                break
            except Exception as e:
                if "503" in str(e):
                    print("Service unavailable, waiting 5 seconds before retry...")
                    time.sleep(5)
                    continue
                raise e
        
        # Save result to JSON
        result = {
            "image": image_key,
            "classification": response.text.strip()
        }
        
        # Append to JSON file
        try:
            with open('house_classifications.json', 'r') as f:
                results = json.load(f)
        except FileNotFoundError:
            results = []
            
        results.append(result)
        
        with open('house_classifications.json', 'w') as f:
            json.dump(results, f, indent=2)
            
        print(f"Processed {image_key}: {response.text.strip()}")
        time.sleep(1)  # Rate limiting
        
    except Exception as e:
        print(f"Error processing {image_key}: {str(e)}")

def main():
    processed_images = get_processed_images()
    # List all objects in the S3 prefix
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].lower().endswith(('.jpg', '.jpeg', '.png')):
                    process_image(obj['Key'], processed_images)

if __name__ == "__main__":
    main()