

from src.IoTnetwork.utils.commons import read_yaml, create_directories
from src.IoTnetwork.constants import *  
from src.IoTnetwork.config_entity.config_params import DataIngestionConfig

class ConfigurationManager:
    def __init__(
        self,
        data_ingestion_config: str = DATA_INGESTION_CONFIG_FILEPATH
    ):

        self.ingestion_config = read_yaml(data_ingestion_config)

        create_directories([self.ingestion_config.artifacts_root])

# Data ingestion config
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        data_config = self.ingestion_config.data_ingestion
        create_directories([data_config.root_dir])

        return DataIngestionConfig(**data_config)