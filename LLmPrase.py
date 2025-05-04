import json
import csv
import google.generativeai as genai
import os
import logging
from google.generativeai import types
import boto3
from botocore.exceptions import NoCredentialsError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='notice_processing.log'
)

# Get API key from AWS Secrets Manager
def get_secret():
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId="gemini/api-key"
        )
        return get_secret_value_response['SecretString']
    except Exception as e:
        logging.error(f"Error fetching secret: {str(e)}")
        raise

api_key = get_secret()
genai.configure(api_key=api_key)

def clean_name(name):
    # Remove periods and extra spaces
    name = name.replace('.', '').strip()
    
    # Split into parts
    parts = name.split()
    
    # Convert each part to title case
    cleaned_parts = []
    for part in parts:
        # Handle initials
        if len(part) == 1:
            cleaned_parts.append(part.upper())
        else:
            cleaned_parts.append(part.title())
    
    return ' '.join(cleaned_parts)

def clean_location(location):
    if not location:
        return '', ''
        
    # Split into city and state
    parts = location.split(',')
    if len(parts) != 2:
        return location, ''
        
    city = parts[0].strip()
    # Get just the state code and convert to uppercase
    state = parts[1].strip().split()[0].upper()[:2]
    
    return city, state

def clean_text(text):
    if not text:
        return ''
    
    # Replace newlines with spaces
    text = text.replace('\n', ' ')
    
    # Replace multiple spaces with single space
    text = ' '.join(text.split())
    
    # Remove any non-printable characters
    text = ''.join(char for char in text if char.isprintable())
    
    return text

def extract_notice_info(notice_text):
    logging.info("Processing new notice")
    prompt = f"""
    Extract information from this legal notice and return it in valid JSON format.
    Return ONLY a JSON object with these keys:
    - deceased_name
    - representative_name
    - deceased_city
    - deceased_state 
    - representative_city
    - representative_state
    - deceased_address
    - representative_address
    - representative_phone
    - is_lawyer: boolean
    
    For states, use only 2-letter state codes (CO, TX, etc).
    If the deceased's city cannot be identified, use the city of the district court mentioned in the notice as the deceased_city.
    For phone numbers, include area code and format as: XXX-XXX-XXXX
    
    Notice text: {notice_text}
    
    Return ONLY the JSON object, no other text.
    """
    
    try:
        print("\n=== INPUT TO LLM ===")
        print(prompt)
        
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content(
            contents=prompt,
            generation_config=types.GenerationConfig(
                max_output_tokens=1000,
                temperature=0.1
            )
        )
        
        print("\n=== RAW LLM RESPONSE ===")
        print(response.text)
        
        # Clean and parse response - remove markdown code block markers
        response_text = response.text.strip()
        if response_text.startswith('```json'):
            response_text = response_text.replace('```json', '', 1)
        if response_text.endswith('```'):
            response_text = response_text[:-3]
        
        response_text = response_text.strip()
        
        print("\n=== CLEANED RESPONSE TEXT ===")
        print(response_text)
        
        result = json.loads(response_text)
        
        # Clean names
        if 'deceased_name' in result:
            result['deceased_name'] = clean_name(result['deceased_name'])
        if 'representative_name' in result:
            result['representative_name'] = clean_name(result['representative_name'])
            
        return result
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON response: {str(e)}")
        logging.error(f"Raw response: {response_text}")
        return {"error": f"Invalid JSON response: {str(e)}"}
    except Exception as e:
        logging.error(f"Error processing notice: {str(e)}")
        return {"error": f"Could not parse notice: {str(e)}"}

# Load the JSON file
try:
    with open('all_notices.json', 'r') as file:
        notices = json.load(file)
        logging.info(f"Successfully loaded {len(notices)} notices")
except Exception as e:
    logging.error(f"Error loading notices file: {str(e)}")
    raise

# Process first 5 notices
results = {}
for i, (notice_id, notice_data) in enumerate(notices.items()):
    logging.info(f"Processing notice {i+1} with ID {notice_id}")
    notice_text = notice_data['notice_text']
    result = extract_notice_info(notice_text)
    results[notice_id] = result
    print(f"\nNotice {i+1}:")
    print(json.dumps(result, indent=2))

# After processing notices, save to both JSON and CSV and upload to S3
try:
    # Save JSON locally first
    json_filename = 'processed_notices.json'
    csv_filename = 'processed_notices.csv'
    
    with open(json_filename, 'w') as file:
        json.dump(results, file, indent=4)
        logging.info("Successfully saved processed notices to JSON locally")
    
    # Save to CSV locally
    with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        header = ['notice_id', 'deceased_name', 'representative_name',
                 'deceased_city', 'deceased_state',
                 'representative_city', 'representative_state',
                 'deceased_address', 'representative_address', 'representative_phone', 'is_lawyer',
                 'published_date', 'notice_text']
        writer.writerow(header)
        
        # Write data
        for notice_id, data in results.items():
            if 'error' not in data:
                row = [
                    notice_id,
                    data.get('deceased_name', ''),
                    data.get('representative_name', ''),
                    data.get('deceased_city', ''),
                    data.get('deceased_state', ''),
                    data.get('representative_city', ''),
                    data.get('representative_state', ''),
                    data.get('deceased_address', ''),
                    data.get('representative_address', ''),
                    data.get('representative_phone', ''),
                    data.get('is_lawyer', ''),
                    notices[notice_id].get('published_date', ''),
                    clean_text(notices[notice_id].get('notice_text', ''))
                ]
                writer.writerow(row)
        logging.info("Successfully saved processed notices to CSV locally")

    # Upload to S3
    s3_client = boto3.client('s3')
    bucket_name = 'datainsdr'
    
    # Upload JSON file
    s3_json_key = 'Processed/processed_notices.json'
    s3_client.upload_file(json_filename, bucket_name, s3_json_key)
    logging.info(f"Successfully uploaded JSON to s3://{bucket_name}/{s3_json_key}")

    # Upload CSV file
    s3_csv_key = 'Processed/processed_notices.csv'
    s3_client.upload_file(csv_filename, bucket_name, s3_csv_key)
    logging.info(f"Successfully uploaded CSV to s3://{bucket_name}/{s3_csv_key}")

    # Clean up local files
    os.remove(json_filename)
    os.remove(csv_filename)
    logging.info("Cleaned up local files")

except NoCredentialsError:
    logging.error("AWS credentials not found")
except Exception as e:
    logging.error(f"Error saving/uploading results: {str(e)}")
