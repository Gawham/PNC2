import pandas as pd

# Read the CSV files
notices_df = pd.read_csv('processed_notices.csv')
addresses_df = pd.read_csv('output.csv')

# Function to find matching address info
def find_address_info(name, addresses_df):
    # Look for exact match in dox_name or Full Name
    match = addresses_df[
        (addresses_df['dox_name'].str.lower() == name.lower()) |
        (addresses_df['Full Name'].str.lower() == name.lower())
    ].iloc[0] if len(addresses_df[
        (addresses_df['dox_name'].str.lower() == name.lower()) |
        (addresses_df['Full Name'].str.lower() == name.lower())
    ]) > 0 else None
    
    if match is not None:
        return match['Current Address'], match['Current Phone'], match['Secondary Phone'], match['Tertiary Phone']
    return None, None, None, None

# Process each row in notices
for idx, row in notices_df.iterrows():
    # Check deceased name
    deceased_name = row['deceased_name']
    address, phone1, phone2, phone3 = find_address_info(deceased_name, addresses_df)
    if address is not None:
        notices_df.at[idx, 'deceased_address'] = address
        notices_df.at[idx, 'deceased_current_phone'] = phone1
        notices_df.at[idx, 'deceased_secondary_phone'] = phone2
        notices_df.at[idx, 'deceased_tertiary_phone'] = phone3
    
    # Check representative name
    rep_name = row['representative_name']
    address, phone1, phone2, phone3 = find_address_info(rep_name, addresses_df)
    if address is not None:
        notices_df.at[idx, 'representative_address'] = address
        # Only update these fields if they are empty or NaN
        if pd.isna(notices_df.at[idx, 'representative_phone']):
            notices_df.at[idx, 'representative_current_phone'] = phone1
            notices_df.at[idx, 'representative_secondary_phone'] = phone2
            notices_df.at[idx, 'representative_tertiary_phone'] = phone3

# Sort the dataframe - non-lawyers first, then lawyers
notices_df = pd.concat([
    notices_df[notices_df['is_lawyer'] != True],
    notices_df[notices_df['is_lawyer'] == True]
])

# Save the updated data
notices_df.to_csv('processed_notices_updated.csv', index=False)
