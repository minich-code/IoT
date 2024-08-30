from src.IoTnetwork.logger import logger
import pandas as pd
from src.IoTnetwork.exception import CustomException
from src.IoTnetwork.config_manager.config_settings import ConfigurationManager
from src.IoTnetwork.components.c_02_data_validation import DataValidation

PIPELINE_NAME = "DATA VALIDATION PIPELINE"

class DataValidationPipeline:
    def __init__(self) -> None:
        self.config_manager = ConfigurationManager()
        self.data_validation_config = self.config_manager.get_data_validation_config()
        self.data_validation = DataValidation(config=self.data_validation_config)

    def run(self) -> None:
        logger.info(f"Starting {PIPELINE_NAME}...")
        try:
            config_manager = ConfigurationManager()
            data_validation_config = config_manager.get_data_validation_config()
            data_validation = DataValidation(config=data_validation_config)

            logger.info("Starting data validation process")
            try:
                data = pd.read_parquet(data_validation_config.data_dir)
            except Exception as e:
                raise CustomException(e, "Error reading parquet file")

            validation_status = data_validation.validate_data(data)

            if validation_status:
                logger.info("Data Validation Completed Successfully!")
            else:
                logger.warning("Data Validation Failed. Check the status file for more details.")
        except CustomException as e:
            logger.error(f"An error occurred while running {PIPELINE_NAME}: {e}")
            raise e


if __name__ == "__main__":
    try:
        logger.info(f"## =================== Starting {PIPELINE_NAME} pipeline ========================##")
        data_validation_pipeline = DataValidationPipeline()
        data_validation_pipeline.run()
        logger.info(f"## =============== {PIPELINE_NAME} Terminated Successfully!=================##\n\nx************************x")
    except Exception as e:
        logger.error(f"Data validation pipeline failed: {e}")
        raise e
