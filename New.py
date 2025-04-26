import json

# Load the data from the JSON files
with open('Max.json', 'r') as file:
    max_data = json.load(file)

with open('26-4.json', 'r') as file:
    data_26_4 = json.load(file)

with open('Done.json', 'r') as file:
    done_data = json.load(file)

# Extract the values from Max.json and convert to integers
max_values = [int(val) for val in max_data['extracted_values']]

# Extract the values from 26-4.json and convert to integers
values_26_4 = [int(val) for val in data_26_4['extracted_values']]

# Extract the ids from Done.json (already integers)
done_ids = done_data['ids']

# Create a set of values to remove (for faster lookup)
values_to_remove = set(values_26_4 + done_ids)

# Filter max_values to keep only those not in values_to_remove
# and remove duplicates by converting to a set and back to a list
filtered_values = list(set([val for val in max_values if val not in values_to_remove]))

# Create the final result
result = {"ALL": filtered_values}

# Write the result to a new JSON file
with open('ALL.json', 'w') as file:
    json.dump(result, file, indent=4)

print(f"Created ALL.json with {len(filtered_values)} unique IDs")
