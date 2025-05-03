import json

# Function to load JSON data from a file
def load_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

# Function to save JSON data to a file
def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

# Load the data from both files
file1_path = '3May.json'
file2_path = '26-4.json'

data1 = load_json(file1_path)
data2 = load_json(file2_path)

# Extract the lists of values
values1 = data1.get('extracted_values', [])
values2 = data2.get('extracted_values', [])

# Convert the second list to a set for efficient lookup
values2_set = set(values2)

# Filter the first list, removing items present in the second list
filtered_values1 = [value for value in values1 if value not in values2_set]

# Create the new data structure for the first file
new_data1 = {'extracted_values': filtered_values1}

# Save the filtered data back to the first file
save_json(file1_path, new_data1)

print(f"Filtered data saved back to {file1_path}")
