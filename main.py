from src.IoTnetwork.logger import logger
from src.IoTnetwork.pipelines.pip_01_data_ingestion import DataIngestionPipeline
from src.IoTnetwork.pipelines.pip_02_data_validation import DataValidationPipeline



COMPONENT_01_NAME = "DATA INGESTION COMPONENT"
try:
    logger.info(f"Starting {COMPONENT_01_NAME}...")
    data_ingestion_pipeline = DataIngestionPipeline()
    data_ingestion_pipeline.run()
    logger.info(f"{COMPONENT_01_NAME} Terminated Successfully! ===============##\n\nx******************x")

except Exception as e:
    logger.error(f"Error in {COMPONENT_01_NAME}: {str(e)}")
    raise e


COMPONENT_02_NAME = "DATA VALIDATION COMPONENT"
try:
    logger.info(f"Starting {COMPONENT_02_NAME}...")
    data_validation_pipeline = DataValidationPipeline()
    data_validation_pipeline.run()
    logger.info(f"{COMPONENT_02_NAME} Terminated Successfully! ===============##\n\nx******************x")

except Exception as e:
    logger.error(f"Error in {COMPONENT_02_NAME}: {str(e)}")
    raise e