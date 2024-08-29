from src.IoTnetwork.logger import logger 
from src.IoTnetwork.exception import CustomException
from src.IoTnetwork.config_manager.config_settings import ConfigurationManager
from src.IoTnetwork.components.c_01_data_ingestion import DataIngestion

PIPELINE_NAME = "DATA INGESTION PIPELINE"


class DataIngestionPipeline:
    def __init__(self):
        pass 

    def run(self):
        logger.info(f"Running {PIPELINE_NAME}...")
        try:
            config_manager = ConfigurationManager()
            data_ingestion_config = config_manager.get_data_ingestion_config()
            data_ingestion = DataIngestion(config=data_ingestion_config)
            data_ingestion.import_data_from_mongodb()
        except CustomException as e:
            logger.error(f"An error occurred while running {PIPELINE_NAME}: {e}")
            raise e
        except Exception as e:
            logger.error(f"An unexpected error occurred while running {PIPELINE_NAME}: {str(e)}")
            raise e
        else:
            logger.info(f"{PIPELINE_NAME} completed successfully.")


if __name__ == "__main__":
    try:
        logger.info(f"## =================== Starting {PIPELINE_NAME} pipeline ========================##")
        data_ingestion_pipeline = DataIngestionPipeline()
        data_ingestion_pipeline.run()
        logger.info(f"## =============== {PIPELINE_NAME} Terminated Successfully!=================##\n\nx************************x")
    except Exception as e:
        logger.error(f"Data ingestion pipeline failed: {e}")
        raise e
