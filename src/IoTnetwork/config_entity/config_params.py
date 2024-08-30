
from dataclasses import dataclass 
from pathlib import Path
@dataclass
class DataIngestionConfig:
    root_dir: Path
    mongo_uri: str
    database_name: str
    collection_name: str
    batch_size: int
    slack_webhook_url: str


@dataclass
class DataValidationConfig:
    root_dir: Path
    val_status: str
    data_dir: Path
    all_schema: dict
    critical_columns: list
    slack_webhook_url: str