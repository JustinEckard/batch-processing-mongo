import pandas as pd
import pymongo
import logging
from pymongo import MongoClient
import time
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables (for secure credential management)
load_dotenv()

def connect_to_mongodb():
    """Establish connection to MongoDB"""
    try:
        # Get connection parameters from environment variables or use defaults
        mongo_host = os.getenv('MONGO_HOST', 'localhost')
        mongo_port = int(os.getenv('MONGO_PORT', 27017))
        mongo_user = os.getenv('MONGO_USER', '')
        mongo_password = os.getenv('MONGO_PASSWORD', '')
        mongo_db = os.getenv('MONGO_DB', 'sensor_database')

        # Build connection string
        if mongo_user and mongo_password:
            connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}"
        else:
            connection_string = f"mongodb://{mongo_host}:{mongo_port}"

        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[mongo_db]
        logger.info("Successfully connected to MongoDB")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def create_database_structure(db):
    """Create collections and indexes if they don't exist"""
    try:
        # Create sensor_readings collection if it doesn't exist
        if "sensor_readings" not in db.list_collection_names():
            db.create_collection("sensor_readings")
            logger.info("Created sensor_readings collection")

        # Create indexes for common query patterns (adjust as needed)
        db.sensor_readings.create_index([("noted_date", pymongo.DESCENDING)])
        db.sensor_readings.create_index([("room_id/id", pymongo.ASCENDING)])
        db.sensor_readings.create_index([("out/in", pymongo.ASCENDING)])
        logger.info("Created indexes for efficient querying")
    except Exception as e:
        logger.error(f"Failed to create database structure: {e}")
        raise

def transform_data(df):
    """Transform the DataFrame to match the target MongoDB document structure."""
    df = df.rename(columns={'id': 'id',
                           'room_id': 'room_id/id',
                           'noted_date': 'noted_date',
                           'temp': 'temp',
                           'out_in': 'out/in'})
    return df

def load_batch_data(db, csv_file_path, batch_size=1000):
    """Load data from CSV file to MongoDB in batches"""
    try:
        # Read the CSV file
        logger.info(f"Reading data from {csv_file_path}")
        df = pd.read_csv(csv_file_path)

        # Transform the DataFrame
        df = transform_data(df.copy())

        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        total_records = len(records)
        logger.info(f"Found {total_records} records to import")

        # Insert in batches
        collection = db.sensor_readings
        for i in range(0, total_records, batch_size):
            batch = records[i:i+batch_size]
            result = collection.insert_many(batch)
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} records")
            # Small delay to prevent overwhelming the database
            time.sleep(0.1)

        logger.info(f"Successfully loaded all {total_records} records")
    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at {csv_file_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to load batch data: {e}")
        raise

def main():
    """Main function to run the data loading process"""
    try:
        # Connect to MongoDB
        db = connect_to_mongodb()

        # Set up database structure
        create_database_structure(db)

        # Load batch data
        csv_file_path = "sensor_data.csv" 
        load_batch_data(db, csv_file_path)

        logger.info("Data loading process completed successfully")
    except Exception as e:
        logger.error(f"Data loading process failed: {e}")

if __name__ == "__main__":
    main()