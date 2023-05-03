from elndata.components.data_extraction import DataExtraction
from elndata.entity.config_entity import Data,DataExtractionConfig
from elndata.entity.artifact_entity import DataExtractionArtifact
from elndata.constant.data import *
from elndata.utils.main import save_csv
from elndata.exception import ELNException
from elndata.logger import logging
import os, sys, shutil
import pandas as pd



class Pipeline:

    def __init__(self):
        self.data_config = Data()

    def start_data_extraction(self)-> DataExtractionArtifact:
        try:
            logging.info('[INFO] Data Extraction from Pipeline Started')
            data_extraction_config = DataExtractionConfig(data_config= self.data_config)
            data_extraction = DataExtraction(data_extraction_config= data_extraction_config)
            data_extraction_artifact = data_extraction.initiate_data_extraction()
            logging.info(f"[INFO] Data Extraction from Pipline completed: {data_extraction_artifact}")

            return data_extraction_artifact

        except Exception as e:
            raise ELNException(e, sys)
    
    def check_if_exist():
        pass

    def compare_changes(self, data_extraction_artifact: DataExtractionArtifact):
        try:
            data_extraction_config = DataExtractionConfig(data_config= self.data_config)
            filter_data_path = os.path.join(data_extraction_config.data_dir)
            file_folder = data_extraction_config.csv_file
            file_name = file_folder.split("\\")[-1]
            pth = os.path.join(filter_data_path, file_name)
            if not os.path.exists(filter_data_path):
                os.makedirs(filter_data_path, exist_ok=True)
                if not os.path.exists(pth):
                    shutil.copy(file_folder, pth)
                    

            # reading extracted data
            extracted_df = pd.read_csv(data_extraction_artifact.data_file)
            col_to_check = extracted_df['editedAt']

            # reading filtered data
            filter_df = pd.read_csv(pth)
            col_with_check = filter_df['editedAt']

            # checking if columns are same or not and replace the file 
            if not col_to_check.equals(col_with_check):
                shutil.copy(file_folder, pth)

        except Exception as e:
            raise ELNException(e, sys)

    
    def run_pipeline(self):
        try:
            data_extraction_artifact = self.start_data_extraction()
            # print(data_extraction_artifact)
            self.compare_changes(data_extraction_artifact= data_extraction_artifact)
        except Exception as e:
            raise ELNException(e, sys)
    