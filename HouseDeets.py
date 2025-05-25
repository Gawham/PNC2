import json
import os
import csv
from curl import cookies, headers, json_data, requests
from datetime import datetime
import time

def get_house_details(address):
    # Update the address in the JSON payload
    json_data['searchFields'][0]['operatorValues'][0]['values'][0] = address
    
    # Make the API request
    response = requests.post('https://prd.realist.com/api/quick-search', 
                            cookies=cookies, 
                            headers=headers, 
                            json=json_data)
    
    return response.json()

def format_date(date_str):
    if date_str and len(date_str) == 8:
        try:
            # Parse YYYYMMDD format
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            # Format as MM/DD/YYYY
            return date_obj.strftime("%m/%d/%Y")
        except ValueError:
            return date_str
    return date_str

def main():
    addresses = []
    processed_ids = set()
    processed_count = 0
    
    csv_path = "processed_notices_updated.csv"
    output_path = 'enriched_pnc.csv'
    
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    
    # First read already processed IDs from output CSV if it exists
    if os.path.exists(output_path):
        with open(output_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'PNC Notice ID' in row:
                    processed_ids.add(row['PNC Notice ID'])
    print(f"Already processed IDs count: {len(processed_ids)}")
    
    # Read input CSV and print fieldnames
    with open(csv_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames.copy()
        print("Input CSV fields:", fieldnames)
        total_rows = 0
        colorado_rows = 0
        for row in reader:
            total_rows += 1
            if row['State Code'] == 'CO' and row['representative_address']:
                colorado_rows += 1
                if 'PNC Notice ID' not in row or row['PNC Notice ID'] not in processed_ids:
                    addresses.append((row, row['representative_address']))
        
        print(f"Total rows in CSV: {total_rows}")
        print(f"Colorado addresses found: {colorado_rows}")
        print(f"New unprocessed Colorado addresses: {len(addresses)}")
    
    print(f"Found {len(addresses)} new addresses to process")
    
    # Add new fields
    fieldnames.extend(['MLS_LISTING_NUMBER', 'SALE_DATE', 'OWNER_NAMES'])
    
    # Create or append to output CSV
    mode = 'a' if os.path.exists(output_path) else 'w'
    with open(output_path, mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        
        for row_data, address in addresses:
            try:
                owner_names = []
                max_retries = 3
                retry_count = 0
                
                while retry_count < max_retries:
                    if retry_count > 0:
                        time.sleep(5)  # 5 second cooldown between retries
                        print(f"Retry #{retry_count + 1} for {address}")
                    
                    response_json = get_house_details(address)
                    print(f"Processed address: {address}")
                    
                    # Extract data from response
                    mls_number = ""
                    sale_date = ""
                    
                    if 'propertySummaryList' in response_json and response_json['propertySummaryList']:
                        property_data = response_json['propertySummaryList'][0].get('propertyData', {})
                        
                        mls_number = property_data.get('MLS_LISTING_NUMBER', '')
                        sale_date = format_date(property_data.get('SALE_DATE', ''))
                        
                        owner_name_1 = property_data.get('OWNER_NAME_1', '')
                        if owner_name_1:
                            owner_names.append(owner_name_1)
                        
                        for i in range(2, 5):
                            owner_key = f'OWNER_NAME_{i}'
                            if owner_key in property_data and property_data[owner_key]:
                                owner_names.append(property_data[owner_key])
                    
                    if owner_names:  # If we got any owner names, break the retry loop
                        break
                    
                    retry_count += 1
                
                row_data['MLS_LISTING_NUMBER'] = mls_number
                row_data['SALE_DATE'] = sale_date
                row_data['OWNER_NAMES'] = '; '.join(owner_names)
                
                writer.writerow(row_data)
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing address {address}: {str(e)}")
                continue
    
    print(f"Enriched data saved to {output_path}")
    print(f"Processed {processed_count} out of {len(addresses)} addresses")

if __name__ == "__main__":
    main()

