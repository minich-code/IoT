from src.IoTnetwork.logger import logger
from src.IoTnetwork.pipelines.pip_01_data_ingestion import DataIngestionPipeline



COMPONENT_01_NAME = "DATA INGESTION COMPONENT"
try:
    logger.info(f"Starting {COMPONENT_01_NAME}...")
    data_ingestion_pipeline = DataIngestionPipeline()
    data_ingestion_pipeline.run()
    logger.info(f"{COMPONENT_01_NAME} Terminated Successfully! ===============##\n\nx******************x")

except Exception as e:
    logger.error(f"Error in {COMPONENT_01_NAME}: {str(e)}")
    raise e