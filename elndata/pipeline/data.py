from elndata.components.data_extraction import DataExtraction
from elndata.entity.config_entity import Data,DataExtractionConfig, DatabaseConfig
from elndata.entity.artifact_entity import DataExtractionArtifact
from elndata.components.postgresql import Database
from elndata.constant.data import *
from elndata.utils.main import save_csv
from elndata.exception import ELNException
from elndata.logger import logging
import os, sys, shutil
import pandas as pd



class Pipeline:

    def __init__(self):
        self.data_config = Data()
        self.database = Database(database_config= DatabaseConfig())

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

            # checking if columns values are same or not and if not same then replace the file 
            if not col_to_check.equals(col_with_check):
                shutil.copy(file_folder, pth)
                return pth
            # this pth is the path of file inside artifact/data_extracted/data.csv
            # if data is changed it will be saved inside this 
            return pth
            # return file_folder

        except Exception as e:
            raise ELNException(e, sys)
    
    def push_to_database(self, extracted_data_file_path: str):
        try:
            logging.info(f"[INFO] Pushing Data to Database")
            self.database.execute(extracted_data_file_path= extracted_data_file_path)
            logging.info(f"[INFO] Data Successfully pushed to database")
        except Exception as e:
            raise ELNException(e, sys)
    
    def run_pipeline(self):
        try:
            data_extraction_artifact = self.start_data_extraction()
            extracted_data_file_path:str = self.compare_changes(data_extraction_artifact= data_extraction_artifact)
            self.push_to_database(extracted_data_file_path = extracted_data_file_path)
            logging.info("[INFO] Data was successfully added to database")
            logging.info("[INFO] Success")
            print('Success!!!')
        except Exception as e:
            raise ELNException(e, sys)
    