import json

# Load the IDs from 3May.json
try:
    with open('3May.json', 'r') as f:
        may_data = json.load(f)
    # Extract the IDs from the "extracted_values" list
    if 'extracted_values' in may_data and isinstance(may_data['extracted_values'], list):
        valid_ids = set(may_data['extracted_values'])
    else:
        print("Error: 3May.json does not have the expected 'extracted_values' structure.")
        exit()
except FileNotFoundError:
    print("Error: 3May.json not found.")
    exit()
except json.JSONDecodeError:
    print("Error: 3May.json is not valid JSON.")
    exit()

# Load all notices
try:
    with open('all_notices.json', 'r') as f:
        all_data = json.load(f)
    if not isinstance(all_data, dict):
        print("Error: all_notices.json does not contain a dictionary.")
        exit()
except FileNotFoundError:
    print("Error: all_notices.json not found.")
    exit()
except json.JSONDecodeError:
    print("Error: all_notices.json is not valid JSON.")
    exit()

# Filter all_notices based on valid IDs
filtered_notices = {
    notice_id: notice_data
    for notice_id, notice_data in all_data.items()
    if notice_id in valid_ids
}

# Write the filtered data back to all_notices.json
try:
    with open('all_noticesCleaned.json', 'w') as f:
        json.dump(filtered_notices, f, indent=4)
    print(f"Successfully filtered all_notices.json. Kept {len(filtered_notices)} entries out of {len(all_data)}.")
except IOError as e:
    print(f"Error writing to all_notices.json: {e}")
