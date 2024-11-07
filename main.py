from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionConfig,DataValidationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig
import sys

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

if __name__ == "__main__":
    try:
        logging.info("Main function started")

        # Instantiate TrainingPipelineConfig
        training_pipeline_config = TrainingPipelineConfig()
        logging.info(f"TrainingPipelineConfig created: {training_pipeline_config}")

        # Instantiate DataIngestionConfig
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        logging.info(f"DataIngestionConfig created: {data_ingestion_config}")

        # Instantiate DataIngestion component
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info("DataIngestion component initialized")

        # Begin data ingestion process
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data ingestion process completed")
        print(data_ingestion_artifact)

        # Instantiate DataValidationConfig
        data_validation_config = DataValidationConfig(training_pipeline_config)
        logging.info(f"DataValidationConfig created: {data_validation_config}")

        # Instantiate DataValidation component
        data_validation = DataValidation(data_ingestion_artifact, data_validation_config)
        logging.info("DataValidation component initialized")

        # Begin data validation process
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("Data validation process completed")
        print(data_validation_artifact)

    except Exception as e:
        error = NetworkSecurityException(e, sys)
        logging.error(f"Error occurred: {error}")
