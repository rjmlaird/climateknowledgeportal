import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import xarray as xr

# S3 client with anonymous access
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
BUCKET = 'wbg-cckp'

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

def download_file(s3_key, local_dir='downloads'):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    filename = s3_key.split('/')[-1]
    local_path = os.path.join(local_dir, filename)
    if os.path.exists(local_path):
        print(f"File already exists, skipping: {local_path}")
        return local_path
    print(f"Downloading {filename}...")
    s3.download_file(BUCKET, s3_key, local_path)
    print(f"Downloaded to {local_path}")
    return local_path

def batch_download_files(file_keys, local_dir='downloads'):
    local_paths = []
    for key in file_keys:
        path = download_file(key, local_dir)
        local_paths.append(path)
    return local_paths

def find_files_recursive(prefix):
    """
    Recursively list all files under the prefix.
    """
    folders, files = list_s3_prefix(prefix)
    all_files = files.copy()

    for folder in folders:
        all_files.extend(find_files_recursive(folder))
    return all_files

def batch_analyze_netcdf(file_paths):
    """
    Example: Open each NetCDF file and print basic info.
    Could be extended to extract specific data or generate summaries.
    """
    for fp in file_paths:
        print(f"\nOpening {fp} for analysis...")
        ds = xr.open_dataset(fp)
        print(ds)
        print("Variables and shapes:")
        for var in ds.data_vars:
            print(f"- {var}: {ds[var].shape}")
        ds.close()

def main_batch():
    # === Define your filters here ===
    # You can also make these dynamic via input() or config files

    collection_filter = 'cmip6-x0.25/'
    variable_filter = 'tas/'
    dataset_filter = 'ensemble-all-historical/'  # e.g. 'access-cm2-r1i1p1f1-historical/'
    # Note: These must include trailing slash because they are prefixes

    prefix = collection_filter + variable_filter + dataset_filter
    print(f"Fetching all NetCDF files under prefix: {prefix}")

    # Recursively find all NetCDF files under prefix
    all_files = find_files_recursive(prefix)

    # Filter to NetCDF files only
    nc_files = [f for f in all_files if f.endswith('.nc')]

    print(f"Found {len(nc_files)} NetCDF files matching filters.")

    # Download all files automatically
    downloaded_files = batch_download_files(nc_files, local_dir='downloads')

    # Optional: run batch analysis on downloaded files
    batch_analyze_netcdf(downloaded_files)

if __name__ == "__main__":
    main_batch()