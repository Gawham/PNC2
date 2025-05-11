import pandas as pd
import re

# Read the CSV file
df = pd.read_csv('processed_notices.csv')

# Function to clean names
def clean_name(name):
    if pd.isna(name):
        return name
    # Keep only text before "/"
    if '/' in name:
        name = name.split('/')[0].strip()
    # Replace hyphens with tildes
    name = name.replace('-', '~')
    # Remove ", Jr", ", Sr", " Jr", " Sr" with case variations
    cleaned = re.sub(r',?\s*[Jj][Rr]\.?(?=\s*$|\s*,)', '', name)
    cleaned = re.sub(r',?\s*[Ss][Rr]\.?(?=\s*$|\s*,)', '', cleaned)
    return cleaned.strip()

# Clean both name columns
df['deceased_name'] = df['deceased_name'].apply(clean_name)
df['representative_name'] = df['representative_name'].apply(clean_name)

# Remove entries where representative_name contains LLC or INC (case insensitive)
df = df[~df['representative_name'].str.contains('LLC|INC', case=False, na=False)]

# Save the cleaned data
df.to_csv('processed_notices_cleaned.csv', index=False)
