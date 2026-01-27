import requests
import os
from pathlib import Path
import argparse

def download_file(url, folder_path, file_name):
    """
    Download OCEL 2.0 Container Logistics JSON file to Databricks volume.
    """
    # Destination path
    destination = os.path.join(folder_path, file_name)
    
    # Create directory if it doesn't exist
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    
    print(f"Starting download from: {url}")
    print(f"Destination: {destination}")
    print(f"Expected file size: 10.2 MB")
    
    try:
        # Download with streaming to handle large file efficiently
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get file size from headers if available
        total_size = int(response.headers.get('content-length', 0))
        
        # Write file in chunks
        chunk_size = 8192
        downloaded = 0
        
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Progress indicator
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}% ({downloaded / 1024 / 1024:.2f} MB)", end='')
        
        print(f"\n✓ Download complete!")
        print(f"✓ File saved to: {destination}")
        print(f"✓ File size: {os.path.getsize(destination) / 1024 / 1024:.2f} MB")
        
        return destination
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error downloading file: {e}")
        raise
    except Exception as e:
        print(f"✗ Error: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download a file')
    parser.add_argument('--url', help='URL to download from.')
    parser.add_argument('--folder_path', help='Destination folder path.')
    parser.add_argument('--file_name', help='Destination file name.')
    args = parser.parse_args()
    
    download_file(args.url, args.folder_path, args.file_name)