import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
import time
from pathlib import Path

# Load environment variables
load_dotenv()

def bing_search(query, subscription_key):
    # Bing Search API endpoint
    endpoint = "https://api.bing.microsoft.com/v7.0/search"
    
    # Request headers
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept-Language": "en-US"
    }
    
    # Query parameters
    params = {
        "q": query,                    # Search query
        "mkt": "en-US",               # Market code
        "count": 50,                  # Number of results (max 50)
        "offset": 0,                  # Starting point of results
        "safeSearch": "Moderate",     # Content filtering
        "textFormat": "Raw"           # Text decoration format
    }
    
    try:
        # Make the request
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        
        # Parse and return the results
        search_results = response.json()
        return search_results
        
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return None

def main():
    # Get API key from environment variables
    subscription_key = os.getenv('BING_API_KEY')
    
    if not subscription_key:
        print("Error: BING_API_KEY not found in environment variables")
        return
    
    # Read the CSV file
    try:
        df = pd.read_csv('enriched_pnc.csv')
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Create RealEstate directory if it doesn't exist
    os.makedirs('RealEstate', exist_ok=True)

    # Process each row
    for index, row in df.iterrows():
        if pd.isna(row['deceased_address']):
            print(f"Skipping row {index}: No address found")
            continue

        address = row['deceased_address']
        notice_id = row['notice_id']
        
        print(f"Processing address for notice_id {notice_id}: {address}")
        
        # Perform the search
        results = bing_search(address, subscription_key)
        
        # Save results to JSON file
        if results:
            output_file = Path('RealEstate') / f"{notice_id}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            print(f"Search results saved to {output_file}")
        else:
            print(f"No results found for notice_id {notice_id}")
        
        # Add a small delay to avoid hitting rate limits
        time.sleep(0.5)

if __name__ == "__main__":
    main()
