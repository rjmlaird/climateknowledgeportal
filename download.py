import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os

BUCKET = 'wbg-cckp'

# Create S3 client with anonymous (unsigned) access
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

def list_s3_prefix(prefix='', delimiter='/'):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter=delimiter)
    folders = []
    files = []
    for page in pages:
        folders.extend(page.get('CommonPrefixes', []))
        files.extend(page.get('Contents', []))
    folder_names = [f['Prefix'] for f in folders]
    file_names = [f['Key'] for f in files]
    return folder_names, file_names

def find_files_recursive(prefix=''):
    """
    Recursively list all files under the prefix.
    """
    folders, files = list_s3_prefix(prefix)
    all_files = files.copy()
    for folder in folders:
        all_files.extend(find_files_recursive(folder))
    return all_files

def download_file(s3_key, local_root='downloads'):
    """
    Download file from S3 keeping the folder structure locally.
    """
    local_path = os.path.join(local_root, s3_key)
    if os.path.exists(local_path):
        print(f"Skipping (exists): {local_path}")
        return
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    print(f"Downloading {s3_key} ...")
    s3.download_file(BUCKET, s3_key, local_path)
    print(f"Saved to {local_path}")

def main():
    print("Listing all files in bucket recursively...")
    all_files = find_files_recursive()
    # Filter for NetCDF files only (.nc)
    nc_files = [f for f in all_files if f.endswith('.nc')]
    print(f"Found {len(nc_files)} NetCDF files to download.")

    for s3_key in nc_files:
        download_file(s3_key)

if __name__ == '__main__':
    main()