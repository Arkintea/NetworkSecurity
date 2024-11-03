import os
import sys
import json
import pandas as pd
import pymongo
import certifi
from dotenv import load_dotenv
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Load environment variables
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

if not MONGO_DB_URL:
    raise ValueError("MONGO_DB_URL is not set in the .env file.")

print("MongoDB URL Loaded:", MONGO_DB_URL)  # Confirm MongoDB URL is loaded

class NetworkDataExtract:
    def __init__(self):
        # Placeholder for future setup if needed
        pass

    def csv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            print(f"Converted {len(records)} records from CSV to JSON.")  # Debugging
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            print("Connecting to MongoDB...")  # Debugging
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=certifi.where())
            self.database = self.mongo_client[database]
            self.collection = self.database[collection]
            result = self.collection.insert_many(records)
            print(f"Inserted {len(result.inserted_ids)} records into MongoDB.")  # Debugging
            return len(result.inserted_ids)
        except pymongo.errors.ServerSelectionTimeoutError as e:
            print("MongoDB Server Selection Timeout Error:", e)
            raise NetworkSecurityException(e, sys)
        except Exception as e:
            print("Error inserting data into MongoDB:", e)
            raise NetworkSecurityException(e, sys)

if __name__ == '__main__':
    FILE_PATH = r"Network_Data\phisingData.csv"  # Use raw string for file path
    DATABASE = "CyberSecurity"  # Database name
    COLLECTION = "NetworkData"  # Collection name
    
    networkobj = NetworkDataExtract()
    
    try:
        records = networkobj.csv_to_json_convertor(file_path=FILE_PATH)
        no_of_records = networkobj.insert_data_mongodb(records, DATABASE, COLLECTION)
        print(f"Successfully inserted {no_of_records} records into MongoDB.")
    except NetworkSecurityException as e:
        print("Exception occurred:", str(e))  # Print the exception message for debugging
