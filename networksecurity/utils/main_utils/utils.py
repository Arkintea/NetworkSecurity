import yaml
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
import os, sys
import numpy as np
import pickle
from sklearn.metrics import r2_score
from sklearn.model_selection import GridSearchCV

# Function to read YAML file
def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path, "r") as yaml_file:  # Corrected to "r" mode for reading YAML files
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise NetworkSecurityException(f"Error reading YAML file at {file_path}: {str(e)}", sys) from e

# Function to write content to YAML file
def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise NetworkSecurityException(f"Error writing YAML file at {file_path}: {str(e)}", sys)

# Function to save a numpy array to file
def save_numpy_array_data(file_path: str, array: np.array):
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise NetworkSecurityException(f"Error saving numpy array to file {file_path}: {str(e)}", sys) from e

# Function to save a Python object to a file
def save_object(file_path: str, obj: object) -> None:
    try:
        logging.info("Saving object to file")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)
        logging.info(f"Object saved to {file_path}")
    except Exception as e:
        raise NetworkSecurityException(f"Error saving object to {file_path}: {str(e)}", sys) from e

# Function to load a Python object from a file
def load_object(file_path: str) -> object:
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist")
        with open(file_path, "rb") as file_obj:
            return pickle.load(file_obj)
    except Exception as e:
        raise NetworkSecurityException(f"Error loading object from file {file_path}: {str(e)}", sys) from e

# Function to load numpy array data from a file
def load_numpy_array_data(file_path: str) -> np.array:
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise NetworkSecurityException(f"Error loading numpy array from file {file_path}: {str(e)}", sys) from e

# Function to evaluate models using GridSearchCV and report their performance
def evaluate_models(X_train, y_train, X_test, y_test, models, param):
    try:
        report = {}

        for model_name, model in models.items():
            logging.info(f"Starting hyperparameter tuning for model: {model_name}")
            parameters = param.get(model_name, {})

            # Perform GridSearchCV
            gs = GridSearchCV(model, parameters, cv=3, n_jobs=-1, verbose=2)  # Added parallelization and verbosity
            gs.fit(X_train, y_train)

            # Set best parameters and retrain the model
            best_model = gs.best_estimator_
            logging.info(f"Best parameters for {model_name}: {gs.best_params_}")

            # Evaluate model performance
            y_train_pred = best_model.predict(X_train)
            y_test_pred = best_model.predict(X_test)

            train_model_score = r2_score(y_train, y_train_pred)
            test_model_score = r2_score(y_test, y_test_pred)

            logging.info(f"{model_name} - Train R2 Score: {train_model_score:.4f}, Test R2 Score: {test_model_score:.4f}")
            report[model_name] = test_model_score

        return report

    except Exception as e:
        raise NetworkSecurityException(f"Error evaluating models: {str(e)}", sys) from e
