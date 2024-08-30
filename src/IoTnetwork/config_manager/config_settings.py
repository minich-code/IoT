

from src.IoTnetwork.utils.commons import read_yaml, create_directories
from src.IoTnetwork.constants import *  
from src.IoTnetwork.logger import logger
from src.IoTnetwork.config_entity.config_params import DataIngestionConfig
from src.IoTnetwork.config_entity.config_params import DataValidationConfig

class ConfigurationManager:
    def __init__(
        self,
        data_ingestion_config: str = DATA_INGESTION_CONFIG_FILEPATH,
        data_validation_config: Path = DATA_VALIDATION_CONFIG_FILEPATH, 
        schema_config: Path = SCHEMA_CONFIG_FILEPATH) -> None:

        logger.info("Initializing Configuration Manager")
        try:
            self.ingestion_config = read_yaml(data_ingestion_config)
            self.data_val_config = read_yaml(data_validation_config)
            self.schema = read_yaml(schema_config)

        except CustomException as e:  
            logger.error(f"Error loading config: {str(e)}")
            raise e

        try:
            create_directories([self.ingestion_config.artifacts_root, self.data_val_config.artifacts_root])

        except CustomException as e:
            logger.error(f"Error creating directories: {str(e)}")
            raise e

        self.ingestion_config_cache = self.ingestion_config
        self.data_val_config_cache = self.data_val_config
        self.schema_cache = self.schema

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        data_config = self.ingestion_config_cache.data_ingestion
        create_directories([data_config.root_dir])

        return DataIngestionConfig(**data_config)

    def get_data_validation_config(self) -> DataValidationConfig:
        data_valid_config = self.data_val_config_cache.data_validation
        schema = self.schema_cache.get('columns', {})
        create_directories([data_valid_config.root_dir])
   
        logger.debug("Data validation configuration loaded")
        return DataValidationConfig(
            root_dir=data_valid_config.root_dir,
            val_status=data_valid_config.val_status,
            data_dir=data_valid_config.data_dir,
            all_schema=schema,
            critical_columns=data_valid_config.critical_columns,
            slack_webhook_url=data_valid_config.slack_webhook_url
        )