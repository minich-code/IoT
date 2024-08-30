from dataclasses import dataclass
from pathlib import Path
import json
import pandas as pd
import numpy as np
import requests  # Add this import for sending Slack notifications
from cerberus import Validator
from scipy.stats import entropy, zscore

from src.IoTnetwork.constants import *
from src.IoTnetwork.logger import logger
from src.IoTnetwork.exception import CustomException
from src.IoTnetwork.config_entity.config_params import DataValidationConfig



class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config
        self.validator = Validator(self.config.all_schema)
        logger.info("DataValidation initialized with config")

    def _send_slack_notification(self, message: str):
        """Sends a notification to Slack."""
        payload = {'text': message}
        try:
            response = requests.post(self.config.slack_webhook_url, json=payload)
            response.raise_for_status()  # Raises an HTTPError if the response was unsuccessful
            logger.info("Slack notification sent successfully")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Slack notification: {e}")

    def _validate_columns(self, data):
        """Validates if all expected columns are present and no unexpected columns exist."""
        all_cols = list(data.columns)
        all_schema = list(self.config.all_schema.keys())

        missing_columns = [col for col in all_schema if col not in all_cols]
        extra_columns = [col for col in all_cols if col not in all_schema]

        error_message = {"missing_columns": missing_columns, "extra_columns": extra_columns}
        if missing_columns or extra_columns:
            logger.debug(f"Validation failed for columns: {error_message}")
            return False, error_message
        return True, None

    def _validate_data_types(self, data):
        """Validates if the data types of each column match the expected schema."""
        type_mapping = {
            "string": "object",
            "integer": "int64",
            "float": "float64",
        }

        all_schema = self.config.all_schema
        type_mismatches = {}
        validation_status = True

        for col, expected_type in all_schema.items():
            if col in data.columns:
                actual_type = str(data[col].dtype)
                if isinstance(expected_type, dict):
                    expected_type = expected_type.get("type", None)
                if not isinstance(expected_type, str):
                    logger.debug(f"Unexpected type format for column '{col}': {expected_type}")
                    expected_type = None
                expected_pandas_type = type_mapping.get(expected_type, None)
                if expected_pandas_type and actual_type != expected_pandas_type:
                    type_mismatches[col] = [expected_type, actual_type]
                    validation_status = False
                    
        if type_mismatches:
            logger.debug(f"Type mismatches: {type_mismatches}")
        return validation_status, type_mismatches

    def _validate_missing_values(self, data):
        """Validates if critical columns have any missing values."""
        missing_values = {}
        for col in self.config.critical_columns:
            if data[col].isnull().sum() > 0:
                missing_values[col] = data[col].isnull().sum()
        if missing_values:
            logger.debug(f"Missing values: {missing_values}")
            return False
        return True

    def _check_cardinality(self, data):
        """Checks and drops columns with unique values."""
        drop_columns = [col for col in data.columns if data[col].nunique() == len(data)]
        data.drop(columns=drop_columns, inplace=True)
        logger.debug(f"Dropped columns with unique values: {drop_columns}")
        return data

    def validate_data(self, data):
        """Performs all data validation checks and returns the overall validation status."""
        validation_results = {}
        
        # Validate all columns
        status, error_message = self._validate_columns(data)
        validation_results["validate_all_columns"] = {"status": status, "error_message": error_message}
        
        # Validate data types
        type_validation_status, type_mismatches = self._validate_data_types(data)
        validation_results["validate_data_types"] = {
            "status": type_validation_status,
            "mismatches": type_mismatches
        }
        
        # Validate missing values
        validation_results["validate_missing_values"] = {"status": self._validate_missing_values(data)}
        
        # Save results to file
        with open(self.config.val_status, 'w') as f:
            json.dump(validation_results, f, indent=4)
        
        overall_validation_status = all(result["status"] for result in validation_results.values())

        # Send Slack notification
        if overall_validation_status:
            message = "Data validation completed successfully."
            logger.info(message)
            self._send_slack_notification(message)
        else:
            message = "Data validation failed. Check the status file for details."
            logger.warning(message)
            self._send_slack_notification(message)
        
        # Save the validated data to a parquet file if validation is successful
        if overall_validation_status:
            output_path = str(Path(self.config.root_dir) / 'iot_data.parquet')
            data.to_parquet(output_path, index=False)
            logger.info(f"Validated data saved to: {output_path}")
        
        return overall_validation_status

