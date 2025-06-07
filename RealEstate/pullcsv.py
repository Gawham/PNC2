import json
import os
import re
import csv

def normalize_address(address):
    # Convert to lowercase and remove extra spaces
    address = address.lower().strip()
    address = re.sub(r'\s+', ' ', address)
    # Remove commas and common words
    address = address.replace(',', '').replace('west', '').replace('east', '')
    address = address.replace('north', '').replace('south', '')
    # Split into components
    return set(address.split())

def find_zillow_url(json_file_path):
    # Read and parse JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    
    # Get original query from JSON and normalize it
    original_query = data['queryContext']['originalQuery']
    query_components = normalize_address(original_query)
    
    # Get all web pages
    web_pages = data['webPages']['value']
    
    # Find Zillow URL that matches the address
    best_match = None
    best_match_score = 0
    
    for page in web_pages:
        if 'zillow.com' in page['url'].lower():
            page_components = normalize_address(page['name'])
            # Calculate match score (intersection of components)
            common_components = query_components.intersection(page_components)
            match_score = len(common_components) / len(query_components)
            
            if match_score > best_match_score:
                best_match_score = match_score
                best_match = page['url']
    
    if best_match_score >= 0.7:  # At least 70% match
        return best_match, best_match_score
    return None, 0

# Create CSV file
with open('real_estate_data.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['ID', 'Address', 'URL'])  # Write header

    # Process all JSON files in RealEstate directory
    directory = 'RealEstate'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_id = filename.replace('.json', '')
            json_file = os.path.join(directory, filename)
            
            with open(json_file, 'r') as f:
                data = json.load(f)
                address = data['queryContext']['originalQuery']
            
            zillow_url, _ = find_zillow_url(json_file)
            csvwriter.writerow([file_id, address, zillow_url if zillow_url else ''])
