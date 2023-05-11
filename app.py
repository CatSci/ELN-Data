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
        p.run_pipeline()
        logging.info("[INFO] Pipeline completed !!!")
    except Exception as e:
        logging.error["[ERROR] Error occurred while running pipeline"]
        raise ELNException(e, sys)

    