import requests
import os
from pathlib import Path
import argparse

class OCEL2JSONUCImporter:
    """
    Represents an Importer for importing an Object-Centric Event Log (OCEL) 2.0-compliant (OCEL2) JSON into Unity Catalog (UC).
    
    Parameters:

    Attributes:
    -----------
    url: str
        A public url an OCEL2 JSON can be downloaded from.
    catalog : str
        The name of a Unity Catalog (UC) catalog an OCEL2 JSON and its extracted data is written to.
    schema : str
        The name of a Unity Catalog (UC) schema within self.<catalog> an OCEL2 JSON and its extracted data is written to.
    volume : str
        The name of a Unity Catalog (UC) volume within self.<catalog>.<schema> an OCEL2 JSON is written to.
    file_name: str
        The name to be given to the OCEL2 JSON file upon being written.

    """
    def __init__(self, url=None, catalog=None, schema=None, volume=None):
        self.url = url
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.file_path = None

    def download_file(self, file_name):
        """
        Download OCEL 2.0 JSON file to Databricks volume.
        """
        if self.url is None:
            raise ValueError("URL is required.")
        if self.catalog is None:
            raise ValueError("Catalog is required.")
        if self.schema is None:
            raise ValueError("Schema is required.")
        if self.volume is None:
            raise ValueError("Volume is required.")
        
        # Create volume directory if directory does not exist
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        Path(volume_path).mkdir(parents=True, exist_ok=True)        

        # Destination file path
        file_path = os.path.join(volume_path, file_name)

        print(f"Starting download from: {self.url}")
        print(f"Destination: {file_path}")
        
        try:
            # Download with streaming to handle large file efficiently
            response = requests.get(self.url, stream=True)
            response.raise_for_status()
            
            # Get file size from headers if available
            total_size = int(response.headers.get('content-length', 0))
            
            # Write file in chunks
            chunk_size = 8192
            downloaded = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Progress indicator
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(f"\rProgress: {progress:.1f}% ({downloaded / 1024 / 1024:.2f} MB)", end='')
            
            print(f"\n✓ Download complete!")
            print(f"✓ File saved to: {file_path}")
            print(f"✓ File size: {os.path.getsize(file_path) / 1024 / 1024:.2f} MB")
            
            return file_path
            
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