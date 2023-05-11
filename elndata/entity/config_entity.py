from elndata.constant import data
from elndata.constant import sql

import os
from datetime import datetime
from dotenv import load_dotenv

class Data:
    def __init__(self, timestamp_format="%m_%d_%Y_%H_%M_%S", timestamp = datetime.now()):
        self.timestamp: str = timestamp.strftime(timestamp_format)
        self.artifact_dir: str = data.ARTIFACT_DIR

class DataExtractionConfig:

    def __init__(self, data_config: Data):
        self.data_extraction_dir: str = os.path.join(data_config.artifact_dir,  data_config.timestamp, data.DATA_EXTRACTION_DIR)
        self.data_dir: str = os.path.join(data_config.artifact_dir, data.DATA_DIR)
        self.final_data:str = data.DATA_DIR
        self.csv_file: str = os.path.join(self.data_extraction_dir, data.CSV_DATA)


class DatabaseConfig:
    def __init__(self, file_pth = data.env_file) -> None:
        load_dotenv(file_pth)
        self.user_name = os.environ.get("USER")
        self.password = os.environ.get("PASSWORD")
        self.database = sql.DATABASE
        self.database_name = sql.DATABASE_NAME
        self.host = sql.HOST
        self.port = sql.PORT





