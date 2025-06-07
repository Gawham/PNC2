import csv
import re
import os
import boto3
import tempfile
from bs4 import BeautifulSoup

def process_html_file(html_file_path):
    try:
        with open(html_file_path, "r", encoding="utf-8") as file:
            html_content = file.read()

        # Parse HTML content
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract search name value
        dox_name = ""
        search_input = soup.find("input", {"id": "search-name-name"})
        if search_input and search_input.has_attr("value"):
            dox_name = search_input["value"]

        # Extract full name
        full_name_section = soup.find("h3", string="Full Name:")
        if full_name_section:
            full_name_text = full_name_section.next_sibling
            full_name = full_name_text.strip() if full_name_text else ""
        else:
            full_name = ""

        # Extract current address
        address_tag = soup.find("h3", string=re.compile("Current Home Address:"))
        if address_tag:
            address_div = address_tag.find_next("div")
            if address_div:
                address_link = address_div.find("a")
                if address_link:
                    address_parts = [part.strip() for part in address_link.stripped_strings]
                    current_address = ", ".join(address_parts)
                else:
                    current_address = ""
        else:
            current_address = ""

        # Extract phone numbers marked as current
        phone_entries = soup.find_all("a", href=re.compile("/\\d{3}-\\d{3}-\\d{4}"))
        current_phone = ""
        secondary_phone = ""
        tertiary_phone = ""
        
        # Debug the HTML structure around phone numbers
        print("Phone entries found:", len(phone_entries))
        
        # First pass: look for current phone
        for i, tag in enumerate(phone_entries):
            phone_number = tag.get_text(strip=True)
            # Look at the next sibling which might contain the "(current)" text
            next_sibling = tag.next_sibling
            next_text = str(next_sibling) if next_sibling else ""
            
            print(f"Phone {i}: {phone_number}, Next text: {next_text[:50]}")
            
            if "(current)" in next_text:
                current_phone = phone_number
                print(f"Found current phone: {current_phone}")
                break
        
        # Second pass: get first non-current phone as secondary and next as tertiary
        if current_phone:
            non_current_count = 0
            for tag in phone_entries:
                phone_number = tag.get_text(strip=True)
                if phone_number != current_phone:
                    if non_current_count == 0:
                        secondary_phone = phone_number
                    elif non_current_count == 1:
                        tertiary_phone = phone_number
                        break
                    non_current_count += 1
        else:
            # If no current phone found, take first three phones in order
            if len(phone_entries) > 0:
                current_phone = phone_entries[0].get_text(strip=True)
            if len(phone_entries) > 1:
                secondary_phone = phone_entries[1].get_text(strip=True)
            if len(phone_entries) > 2:
                tertiary_phone = phone_entries[2].get_text(strip=True)
        
        return [dox_name, full_name, current_address, current_phone, secondary_phone, tertiary_phone]
    except Exception as e:
        print(f"Error processing {html_file_path}: {e}")
        return None

def main():
    bucket_name = "datainsdr"
    prefix = "PNCBigBoy6thJune/"
    processed_count = 0
    output_file = "output.csv"
    no_matches = []  # New list to track files with no matches
    
    s3_client = boto3.client('s3')
    
    # Create CSV file and write header
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Dox Name', 'Full Name', 'Current Address', 'Current Phone', 'Secondary Phone', 'Tertiary Phone'])
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        objects = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in objects:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                if not obj['Key'].endswith('.html'):
                    continue
                    
                print(f"\nProcessing {obj['Key']}...")
                with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as temp_file:
                    temp_file_path = temp_file.name
                
                try:
                    s3_client.download_file(bucket_name, obj['Key'], temp_file_path)
                    result = process_html_file(temp_file_path)
                    if result:
                        # Append to CSV
                        with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(result)
                        
                        print("Parsed data:")
                        print("Dox Name:", result[0])
                        print("Full Name:", result[1]) 
                        print("Current Address:", result[2])
                        print("Current Phone:", result[3])
                        print("Secondary Phone:", result[4])
                        print("Tertiary Phone:", result[5])
                    else:
                        no_matches.append(obj['Key'])  # Add file to no matches list
                finally:
                    os.unlink(temp_file_path)
                
                processed_count += 1
        
        print(f"\nProcessed {processed_count} files. Results saved to {output_file}")
        if no_matches:  # Print files with no matches
            print("\nFiles with no matches:")
            for file in no_matches:
                print(file)
                    
    except Exception as e:
        print(f"Error: {e}")
    
    # Print no matches if we didn't hit max_files
    if no_matches:
        print("\nFiles with no matches:")
        for file in no_matches:
            print(file)

if __name__ == "__main__":
    main()
