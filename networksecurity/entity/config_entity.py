from datetime import datetime
import os
from networksecurity.constant import training_pipeline

# Debugging: Check if all constants are properly defined in training_pipeline
try:
    assert training_pipeline.PIPELINE_NAME is not None
    assert training_pipeline.ARTIFACT_DIR is not None
    assert training_pipeline.DATA_INGESTION_DIR_NAME is not None
    assert training_pipeline.FILE_NAME is not None
    assert training_pipeline.DATA_INGESTION_FEATURE_STORE_DIR_NAME is not None
    assert training_pipeline.DATA_INGESTION_INGESTED_DIR_NAME is not None
    assert training_pipeline.TRAIN_FILE_NAME is not None
    assert training_pipeline.TEST_FILE_NAME is not None
    assert training_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO is not None
    assert training_pipeline.DATA_INGESTION_COLLECTION_NAME is not None
    assert training_pipeline.DATA_INGESTION_DATABASE_NAME is not None
    assert training_pipeline.TARGET_COLUMNS is not None
except AssertionError:
    print("One or more training_pipeline constants are undefined or None")

class TrainingPipelineConfig:
    def __init__(self, timestamp=datetime.now()):
        timestamp = timestamp.strftime("%m_%d_%Y_%H_%M_%S")
        self.pipeline_name = training_pipeline.PIPELINE_NAME
        self.artifact_name = training_pipeline.ARTIFACT_DIR
        self.artifact_dir = os.path.join(self.artifact_name, timestamp)
        self.model_dir = os.path.join("final_model")
        self.timestamp: str = timestamp

class DataIngestionConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.data_ingestion_dir: str = os.path.join(training_pipeline_config.artifact_dir, training_pipeline.DATA_INGESTION_DIR_NAME)
        self.feature_store_file_path: str = os.path.join(self.data_ingestion_dir, training_pipeline.DATA_INGESTION_FEATURE_STORE_DIR_NAME, training_pipeline.FILE_NAME)
        self.training_file_path: str = os.path.join(self.data_ingestion_dir, training_pipeline.DATA_INGESTION_INGESTED_DIR_NAME, training_pipeline.TRAIN_FILE_NAME)
        self.testing_file_path: str = os.path.join(self.data_ingestion_dir, training_pipeline.DATA_INGESTION_INGESTED_DIR_NAME, training_pipeline.TEST_FILE_NAME)
        self.train_test_split_ratio: float = training_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
        self.collection_name: str = training_pipeline.DATA_INGESTION_COLLECTION_NAME
        self.database_name: str = training_pipeline.DATA_INGESTION_DATABASE_NAME

try:
    assert training_pipeline.DATA_VALIDATION_DIR_NAME is not None
    assert training_pipeline.DATA_VALIDATION_INVALID_DIR_NAME is not None
    assert training_pipeline.DATA_VALIDATION_VALID_DIR_NAME is not None
    assert training_pipeline.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME is not None
    assert training_pipeline.DATA_VALIDATION_DRIFT_REPORT_DIR_NAME is not None
except AssertionError:
    print("One or more training_pipeline constants are undefined or None")

class DataValidationConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.data_validation_dir: str = os.path.join(training_pipeline_config.artifact_dir, training_pipeline.DATA_VALIDATION_DIR_NAME)
        self.valid_data_dir: str = os.path.join(self.data_validation_dir, training_pipeline.DATA_VALIDATION_VALID_DIR_NAME)
        self.invalid_data_dir: str = os.path.join(self.data_validation_dir, training_pipeline.DATA_VALIDATION_INVALID_DIR_NAME)
        self.valid_train_file_path: str = os.path.join(self.valid_data_dir, training_pipeline.TRAIN_FILE_NAME)
        self.valid_test_file_path: str = os.path.join(self.valid_data_dir, training_pipeline.TEST_FILE_NAME)
        self.invalid_train_file_path: str = os.path.join(self.invalid_data_dir, training_pipeline.TRAIN_FILE_NAME)
        self.invalid_test_file_path: str = os.path.join(self.invalid_data_dir, training_pipeline.TEST_FILE_NAME)
        self.drift_report_file_path: str = os.path.join(self.data_validation_dir, training_pipeline.DATA_VALIDATION_DRIFT_REPORT_DIR_NAME, training_pipeline.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME)