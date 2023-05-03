from elndata.constant.data import SCHEMA_FILE_PATH
from elndata.exception import ELNException
from elndata.logger import logging
from elndata.utils.main import read_yaml_file
from elndata.components.data_extraction import DataExtraction
from elndata.entity.config_entity import Data,DataExtractionConfig
from elndata.pipeline.data import Pipeline
from elndata.constant.data import *
import yaml
import sys, requests
from datetime import datetime
import pandas as pd



if __name__ == "__main__":

    try:
        p = Pipeline()
        logging.info('[INFO] Extracting data from ELN started')
        # data_extraction_config = DataExtractionConfig(data_config= Data())
        # d = DataExtraction(data_extracttion_config= data_extraction_config)
        # d.initiate_data_extraction()
        p.run_pipeline()
        logging.info("[INFO] Extracting data from ELN completed")
    except Exception as e:
        logging.error["[ERROR] Error occurred while extarcting data"]
        raise ELNException(e, sys)

    