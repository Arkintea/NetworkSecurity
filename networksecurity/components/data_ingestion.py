from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from sklearn.model_selection import train_test_split
import sys, os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import pymongo

# Load environment variables and check MONGO_DB_URL
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(f"MONGO_DB_URL is loaded")

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            logging.info(f"{'>>' * 20}Data Ingestion log started.{'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def export_collection_as_dataframe(self, collection_name: str, database_name: str) -> pd.DataFrame:
        try:
            logging.info(f"Exporting collection [{collection_name}] from [{database_name}] to DataFrame")
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            self.database = self.mongo_client[database_name]
            self.collection = self.database[collection_name]
            data_frame=pd.DataFrame(list(self.collection.find()))
            if "_id" in data_frame.columns.to_list():
                data_frame=data_frame.drop(columns=["_id"],axis=1)
            
            data_frame.replace({"na":np.nan},inplace=True)
            return data_frame
        
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def export_data_into_feature_store(self, dataframe: pd.DataFrame) -> None:
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def split_data_as_train_test(self, dataframe: pd.DataFrame) -> None:
        try:
            train_test_split_ratio = self.data_ingestion_config.train_test_split_ratio
            train_set, test_set = train_test_split(dataframe, test_size=train_test_split_ratio)
            logging.info("train test split completed")
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)
            logging.info("Exporting training data")
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            logging.info(f"train file is saved at {self.data_ingestion_config.training_file_path}")
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)
            logging.info(f"test file is saved at {self.data_ingestion_config.testing_file_path}")
            logging.info(f"Exported train and test file path")
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logging.info("Starting the data ingestion")
            dataframe = self.export_collection_as_dataframe(
                collection_name=self.data_ingestion_config.collection_name,
                database_name=self.data_ingestion_config.database_name
            )
            logging.info("Exported data from MongoDB to DataFrame")
            self.export_data_into_feature_store(dataframe)
            logging.info("Exported data into feature store")
            self.split_data_as_train_test(dataframe)
            logging.info("Splitting data into train and test")
            data_ingestion_artifact = DataIngestionArtifact(
                feature_store_path=self.data_ingestion_config.feature_store_file_path,
                train_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            logging.info(f"Data ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e
