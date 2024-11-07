from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_NAME
from scipy.stats import ks_2samp
import pandas as pd
import os, sys
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_NAME)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self._schema_config['columns'])  # Assuming schema config has a 'columns' key
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"DataFrame has columns: {len(dataframe.columns)}")
            return len(dataframe.columns) == number_of_columns
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold: float = 0.05) -> bool:
        try:
            status = True
            report = {}

            for column in base_df.columns:
                if column not in current_df.columns:
                    raise NetworkSecurityException(f"Column '{column}' is missing in the current dataset.", sys)
                
                d1 = base_df[column]
                d2 = current_df[column]
                ks_test_result = ks_2samp(d1, d2)

                is_found = ks_test_result.pvalue < threshold
                status = status and not is_found  # Invalidate status if any drift found
                
                report[column] = {
                    "p_value": float(ks_test_result.pvalue),
                    "drift_status": is_found
                }

            # Write drift report to YAML
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)

            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Read train and test data
            train_dataframe = self.read_data(train_file_path)
            test_dataframe = self.read_data(test_file_path)

            # Validate number of columns
            if not self.validate_number_of_columns(train_dataframe):
                raise NetworkSecurityException("Train DataFrame does not contain the expected columns.", sys)
            if not self.validate_number_of_columns(test_dataframe):
                raise NetworkSecurityException("Test DataFrame does not contain the expected columns.", sys)

            # Check data drift
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)

            # Save validated data
            os.makedirs(os.path.dirname(self.data_validation_config.valid_train_file_path), exist_ok=True)
            train_dataframe.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            test_dataframe.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)

            # Create DataValidationArtifact
            return DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
        except Exception as e:
            raise NetworkSecurityException(e, sys)
