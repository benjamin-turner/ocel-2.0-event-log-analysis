import argparse
import requests
import os
import json
from pathlib import Path

from typing import Dict, List, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, lit

class OCEL2UCImporter:
    """
    An Unity Catalog (UC) importer for an Object-Centric Event Log (OCEL) 2.0-compliant (OCEL2) JSON.

    Attributes:
    -----------
    url: str
        A public url an OCEL2 JSON is to be downloaded from.
    catalog : str
        The name of a Unity Catalog (UC) <catalog> a file is/to be written within.
    schema : str
        The name of a Unity Catalog (UC) <schema> in a <catalog> a file is/to be written within.
    volume : str
        The name of a Unity Catalog (UC) <volume> in a <catalog>.<schema> a file is/to be writtin within.
    file_name: str
        The name to be given to a file written to a Unity Catalog (UC) volume.
    file_path: str
        The Unity Catalog (UC) volume file_path to which a file is/to be written to.
    """
    def __init__(self):
        self.catalog = None
        self.schema = None
        self.volume = None
        self.file_name = None
        self.file_path = None

    def parse_uc_file_path(file_path):
        """Parses a file's Unity Catalog (UC) volume file_path into a list of its elements.
        
        Args:
            file_path (str): The file's Unity Catalog (UC) volume file_path. Expected format: '/Volumes/<catalog>/<schema>/<volume>/<file_name>'.

        Returns:
            A dictionary with keys 'catalog', 'schema', 'volume', and 'file_name'.

        Raises:
            ValueError: If the file_path is not in the expected format.
        """
        try:
            catalog, schema, volume, file_name = file_path.split('/')[2:]
            return {
                'catalog': catalog,
                'schema': schema,
                'volume': volume,
                'file_name': file_name
            }
        except ValueError:
            raise ValueError("Invalid file path format. Expected format: '/Volumes/<catalog>/<schema>/<volume>/<file_name>'.")                                                          
    
    def set_uc_file_path(self, file_path=None, catalog=None, schema=None, volume=None, file_name=None):
        """Sets a file's Unity Catalog (UC) volume file_path.
        
        Provide either 'file_path', or all of (catalog, schema, volume, file_name) for 'file_path' to be constructed and set.
        
        Args:
            file_path (str): The file's Unity Catalog (UC) volume file_path. Expected format: '/Volumes/<catalog>/<schema>/<volume>/<file_name>'.
            catalog (str): The Unity Catalog (UC) catalog a file is written to.
            schema (str): The Unity Catalog (UC) schema a file is written to.
            volume (str): The Unity Catalog (UC) volume a file is written to.
            file_name (str): The name to be given to a file written to a Unity Catalog (UC) volume.

        Raises:
            ValueError: If neither file_path nor all of (catalog, schema, volume, file_name) are provided, or if both file_path and any component are provided. Also raised if file_path is invalid (see parse_uc_file_path()).
        """
        
        # Check for mutually exlusive usage
        has_file_path = file_path is not None
        has_components = any(x is not None for x in [catalog, schema, volume, file_name])

        if has_file_path and has_components:
            raise ValueError(
                "Provide either 'file_path' or all of (catalog, schema, volume, file_name), but not both."    
            )

        if not has_file_path and not has_components:
            raise ValueError(
                "Must provide either 'file_path' or all of (catalog, schema, volume, file_name)."
            )

        # Parse from file_path
        if has_file_path:
            file_path_elements = parse_uc_file_path(file_path)
            self.catalog = file_path_elements['catalog']
            self.schema = file_path_elements['schema']
            self.volume = file_path_elements['volume']
            self.file_name = file_path_elements['file_name']
            self.file_path = file_path
            return
        
        # Validate all components are provided
        missing = [name for name, value in [
            ('catalog', catalog), 
            ('schema', schema), 
            ('volume', volume), 
            ('file_name', file_name)
        ] if value is None]
    
        if missing:
            raise ValueError(
                f"When not providing file_path, all components are required. "
                f"Missing: {', '.join(missing)}"
            )

        # Set from components
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.file_name = file_name
        self.file_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_name}"
        return

    def download_file(self, url, file_path=None):
        """
        Downloads OCEL 2.0 JSON file to Databricks volume.
        """

        if url is None:
            raise ValueError("Parameter 'url' is required.")
        if file_path is None:
            if self.file_path is not None:
                file_path = self.file_path
            else:
                raise ValueError("Parameter 'file_path' is required. Either pass it as a parameter, or set the instance attribute method set_file_path(self, catalog, schema, volume, file_name) to set the instance file_path attribute or" )

        print(f"Starting download from: {url}")
        print(f"Destination: {file_path}")
        
        try:
            # Download with streaming to handle large file efficiently
            response = requests.get(url, stream=True)
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
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error downloading file: {e}")
            raise
        except Exception as e:
            print(f"✗ Error: {e}")
            raise

    def _load_ocel_json(self, json_path: str) -> Dict[str, Any]:
        """Load OCEL 2.0 JSON file."""
        with open(json_path, 'r') as f:
            return json.load(f)
    
    def _create_and_load_object_types_table(self, ocel_data: Dict) -> None:
        """
        Create object_types table.
        Schema: object_type_name, attribute_name, attribute_type
        """
        rows = []
        for obj_type in ocel_data['objectTypes']:
            type_name = obj_type['name']
            for attr in obj_type['attributes']:
                rows.append({
                    'object_type_name': type_name,
                    'attribute_name': attr['name'],
                    'attribute_type': attr['type']
                })
        
        if rows:
            df = self.spark.createDataFrame(rows)
        else:
            # Handle case where there are no attributes
            schema = StructType([
                StructField("object_type_name", StringType(), True),
                StructField("attribute_name", StringType(), True),
                StructField("attribute_type", StringType(), True)
            ])
            df = self.spark.createDataFrame([], schema)
        
        table_name = f"{self.full_schema}.object_types"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def _create_and_load_event_types_table(self, ocel_data: Dict) -> None:
        """
        Create event_types table.
        Schema: event_type_name, attribute_name, attribute_type
        """
        rows = []
        for event_type in ocel_data['eventTypes']:
            type_name = event_type['name']
            for attr in event_type['attributes']:
                rows.append({
                    'event_type_name': type_name,
                    'attribute_name': attr['name'],
                    'attribute_type': attr['type']
                })
        
        if rows:
            df = self.spark.createDataFrame(rows)
        else:
            schema = StructType([
                StructField("event_type_name", StringType(), True),
                StructField("attribute_name", StringType(), True),
                StructField("attribute_type", StringType(), True)
            ])
            df = self.spark.createDataFrame([], schema)
        
        table_name = f"{self.full_schema}.event_types"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def create_and_load_objects_table(self, ocel_data: Dict) -> None:
        """
        Create objects table.
        Schema: object_id, object_type
        """
        rows = []
        for obj in ocel_data['objects']:
            rows.append({
                'object_id': obj['id'],
                'object_type': obj['type']
            })
        
        df = self.spark.createDataFrame(rows)
        table_name = f"{self.full_schema}.objects"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def _create_and_load_attributes_table(self, ocel_data: Dict) -> None:
        """
        Create object_attributes table.
        Schema: object_id, attribute_name, attribute_value, attribute_time
        """
        rows = []
        for obj in ocel_data['objects']:
            obj_id = obj['id']
            for attr in obj.get('attributes', []):
                rows.append({
                    'object_id': obj_id,
                    'attribute_name': attr['name'],
                    'attribute_value': str(attr['value']),  # Store as string for flexibility
                    'attribute_time': attr.get('time')
                })
        
        if rows:
            df = self.spark.createDataFrame(rows)
        else:
            schema = StructType([
                StructField("object_id", StringType(), True),
                StructField("attribute_name", StringType(), True),
                StructField("attribute_value", StringType(), True),
                StructField("attribute_time", StringType(), True)
            ])
            df = self.spark.createDataFrame([], schema)
        
        table_name = f"{self.full_schema}.object_attributes"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def _create_and_load_object_relationships_table(self, ocel_data: Dict) -> None:
        """
        Create object_relationships table (object-to-object relationships).
        Schema: source_object_id, target_object_id, qualifier
        """
        rows = []
        for obj in ocel_data['objects']:
            source_id = obj['id']
            for rel in obj.get('relationships', []):
                rows.append({
                    'source_object_id': source_id,
                    'target_object_id': rel['objectId'],
                    'qualifier': rel.get('qualifier', '')
                })
        
        if rows:
            df = self.spark.createDataFrame(rows)
        else:
            schema = StructType([
                StructField("source_object_id", StringType(), True),
                StructField("target_object_id", StringType(), True),
                StructField("qualifier", StringType(), True)
            ])
            df = self.spark.createDataFrame([], schema)
        
        table_name = f"{self.full_schema}.object_relationships"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def _create_and_load_events_table(self, ocel_data: Dict) -> None:
        """
        Create events table.
        Schema: event_id, event_type, event_time
        """
        rows = []
        for event in ocel_data['events']:
            rows.append({
                'event_id': event['id'],
                'event_type': event['type'],
                'event_time': event['time']
            })
        
        df = self.spark.createDataFrame(rows)
        table_name = f"{self.full_schema}.events"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def _create_and_load_event_attributes_table(self, ocel_data: Dict) -> None:
        """
        Create event_attributes table.
        Schema: event_id, attribute_name, attribute_value
        """
        rows = []
        for event in ocel_data['events']:
            event_id = event['id']
            for attr in event.get('attributes', []):
                rows.append({
                    'event_id': event_id,
                    'attribute_name': attr['name'],
                    'attribute_value': str(attr['value'])
                })
        
        if rows:
            df = self.spark.createDataFrame(rows)
        else:
            schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("attribute_name", StringType(), True),
                StructField("attribute_value", StringType(), True)
            ])
            df = self.spark.createDataFrame([], schema)
        
        table_name = f"{self.full_schema}.event_attributes"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")
    
    def _create_and_load_event_object_relationships_table(self, ocel_data: Dict) -> None:
        """
        Create event_object_relationships table (event-to-object relationships).
        Schema: event_id, object_id, qualifier
        """
        rows = []
        for event in ocel_data['events']:
            event_id = event['id']
            for rel in event.get('relationships', []):
                rows.append({
                    'event_id': event_id,
                    'object_id': rel['objectId'],
                    'qualifier': rel.get('qualifier', '')
                })
        
        df = self.spark.createDataFrame(rows)
        table_name = f"{self.full_schema}.event_object_relationships"
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✓ Created table: {table_name} ({df.count()} rows)")

    def write_ocel2_json_to_schema(self) -> None:
        """
        Execute full conversion from OCEL 2.0 JSON to Unity Catalog tables.
        
        Args:
            json_path: Path to the OCEL 2.0 JSON file
        """
        if self.file_path is None:
            raise ValueError("Class attribute 'file_path' is required. Use method set_file_path() to set it")
        
        print(f"Loading OCEL 2.0 data from {self.file_path}...")
        ocel2_json = self._load_ocel_json(json_path)
        
        print(f"\nCreating tables in {self.catalog}.{self.schema}...\n")
        
        # Create all tables
        self._create_and_load_object_types_table(ocel2_json)
        self._create_and_load_event_types_table(ocel2_json)
        self._create_and_load_objects_table(ocel2_json)
        self._create_and_load_object_attributes_table(ocel2_json)
        self._create_and_load_object_relationships_table(ocel2_json)
        self._create_and_load_events_table(ocel2_json)
        self._create_and_load_event_attributes_table(ocel2_json)
        self._create_and_load_event_object_relationships_table(ocel2_json)
        
        print(f"\n✅ Successfully created all tables in {self.full_schema}")
        print("\nTable Schema Summary:")
        print("=" * 80)
        print("1. object_types: object_type_name, attribute_name, attribute_type")
        print("2. event_types: event_type_name, attribute_name, attribute_type")
        print("3. objects: object_id, object_type")
        print("4. object_attributes: object_id, attribute_name, attribute_value, attribute_time")
        print("5. object_relationships: source_object_id, target_object_id, qualifier")
        print("6. events: event_id, event_type, event_time")
        print("7. event_attributes: event_id, attribute_name, attribute_value")
        print("8. event_object_relationships: event_id, object_id, qualifier")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download a file')
    parser.add_argument('--url', help='URL to download from.')
    parser.add_argument('--folder_path', help='Destination folder path.')
    parser.add_argument('--file_name', help='Destination file name.')
    args = parser.parse_args()
    
    download_file(args.url, args.folder_path, args.file_name)