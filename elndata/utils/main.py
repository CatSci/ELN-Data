from elndata.exception import ELNException
from elndata.logger import logging
from elndata.constant.data import SCHEMA_FILE_PATH
import pandas as pd

import os, sys
import yaml

def read_yaml_file(file_path: str) -> dict:
    try:
        logging.info('[INFO] Reading YAML file')
        with open(file_path, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        logging.error("[ERROR] error occurred while reading YAML file")
        raise ELNException(e, sys) from e

def write_yaml_file(file_path: str, content: object, replace: bool = False):

    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)
        
        os.makedirs(os.path.dirname(file_path), exist_ok = True)

        with open(file_path, 'w') as yaml_file:
            yaml.dump(content, yaml_file)
    except Exception as e:
        raise ELNException(e, sys)
    

def save_csv(file_path: str, 
             dataframe: pd.DataFrame, 
             replace: bool = False):
    try:
        if replace and os.path.exists(file_path):
            os.remove(file_path)
        
        os.makedirs(os.path.dirname(file_path), exist_ok = True)

        return dataframe.to_csv(file_path, index = False)
    except Exception as e:
        raise ELNException(e, sys)