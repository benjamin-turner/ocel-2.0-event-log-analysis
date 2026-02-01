"""
OCEL 2.0 to Databricks Unity Catalog Relational Tables
This script unnests OCEL 2.0 JSON data into normalized relational tables.
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, lit
from typing import Dict, List, Any
import argparse

class OCELUCSchema:
    """
    Converts OCEL 2.0 JSON format to Unity Catalog relational tables.
    """
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        """
        Initialize the converter.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            schema: Schema name within the catalog
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        
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
    
    def write_ocel_json_to_schema(self, json_path: str) -> None:
        """
        Execute full conversion from OCEL 2.0 JSON to Unity Catalog tables.
        
        Args:
            json_path: Path to the OCEL 2.0 JSON file
        """
        print(f"Loading OCEL 2.0 data from {json_path}...")
        ocel_data = self._load_ocel_json(json_path)
        
        print(f"\nCreating tables in {self.full_schema}...\n")
        
        # Create all tables
        self._create_and_load_object_types_table(ocel_data)
        self._create_and_load_event_types_table(ocel_data)
        self._create_and_load_objects_table(ocel_data)
        self._create_and_load_object_attributes_table(ocel_data)
        self._create_and_load_object_relationships_table(ocel_data)
        self._create_and_load_events_table(ocel_data)
        self._create_and_load_event_attributes_table(ocel_data)
        self._create_and_load_event_object_relationships_table(ocel_data)
        
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
    parser = argparse.ArgumentParser(description='Convert OCEL 2.0 JSON to Unity Catalog tables.')
    parser.add_argument('--catalog', type=str, required=True, help='Unity Catalog name')
    parser.add_argument('--schema', type=str, required=True, help='Unity Catalog schema name')
    parser.add_argument('--json_path', type=str, required=True, help='Path to the OCEL 2.0 JSON file')
    args = parser.parse_args()
    
    converter = OCELUCSchema(spark, args.catalog, args.schema)
    converter.write_ocel_json_to_schema(args.json_path)