import os
from google.cloud import storage
from datetime import datetime, timezone
from google.cloud.exceptions import GoogleCloudError

def upload_to_bucket(bucket_name, local_file_path, destination_blob_name, metadata=None):
    try:
        storage_client = storage.Client.from_service_account_json('tabsons-f3426d01189d.json')
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Set metadata for the blob
        if metadata:
            blob.metadata = metadata
        
        # Upload the file
        with open(local_file_path, 'rb') as f:
            blob.upload_from_file(f)
        
        print(f"File {local_file_path} uploaded to {bucket_name}/{destination_blob_name}.")
        return blob.public_url
    except GoogleCloudError as e:
        print(f"Failed to upload {local_file_path} to {bucket_name}/{destination_blob_name}: {e}")
        return None

def generate_blob_name(local_file_path, folder, segment=None):
    # Generate a blob name based on the local file path to maintain directory structure
    file_name = os.path.basename(local_file_path)
    local_dir = os.path.dirname(local_file_path)
    
    if folder == 'Image' and segment:
        return f"{folder}/{segment}/{file_name}"
    else:
        return f"{folder}/{file_name}"

def upload_files_to_buckets(local_file_path, folder, segment=None):
    blob_name = generate_blob_name(local_file_path, folder, segment)
    
    # Define metadata for the file
    metadata = {
        'file_type': folder,
        'segment': segment,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    return upload_to_bucket('imagestg-bucket', local_file_path, blob_name, metadata)

