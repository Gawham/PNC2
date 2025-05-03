import boto3
import os

def duplicate_folder():
    # Initialize S3 client
    s3 = boto3.client('s3')
    bucket_name = 'datainsdr'
    source_prefix = 'PNC26-4/'
    destination_prefix = 'PNC26-4-backup/'

    try:
        # List all objects in source prefix
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key']
                    # Create new destination key
                    destination_key = source_key.replace(source_prefix, destination_prefix, 1)
                    
                    # Copy object to new location
                    copy_source = {'Bucket': bucket_name, 'Key': source_key}
                    print(f"Copying {source_key} to {destination_key}")
                    s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)

        print("Folder structure duplicated successfully")
        
    except Exception as e:
        print(f"Error during duplication: {e}")

if __name__ == "__main__":
    duplicate_folder()
