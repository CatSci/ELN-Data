from elndata.constant import data

import os
from datetime import datetime

class Data:
    def __init__(self, timestamp_format="%m_%d_%Y_%H_%M_%S", timestamp = datetime.now()):
        self.timestamp: str = timestamp.strftime(timestamp_format)
        self.artifact_dir: str = data.ARTIFACT_DIR

class DataExtractionConfig:

    def __init__(self, data_config: Data):
        self.data_extraction_dir: str = os.path.join(data_config.artifact_dir,  data_config.timestamp, data.DATA_EXTRACTION_DIR)
        self.data_dir: str = os.path.join(data_config.artifact_dir, data.DATA_DIR)
        self.csv_file: str = os.path.join(self.data_extraction_dir, data.CSV_DATA)





