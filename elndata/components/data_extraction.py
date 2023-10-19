from elndata.entity.artifact_entity import DataExtractionArtifact
from elndata.entity.config_entity import Data,DataExtractionConfig
from elndata.utils.main import read_yaml_file, write_yaml_file, save_csv
from elndata.constant.data import SCHEMA_FILE_PATH, DATA_DIR
from elndata.constant.eln import BASE_URL


from elndata.exception import ELNException
from elndata.logger import logging

import os, sys, requests, datetime
import pandas as pd

from collections import OrderedDict
from dotenv import load_dotenv

import csv

filename = 'text.csv'

def save_text_to_csv(text, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Text'])
        writer.writerow([text])

class DataExtraction:
    def __init__(self, data_extraction_config: DataExtractionConfig,
                 url = BASE_URL, 
                 offset: int = 0,
                 num_of_exp: int = 20,
                 includes_type: str = "experiment,paraexp"):
        try:
            # self.data_extraction_config  = DataExtractionConfig(data_config= Data())
            self.data_extraction_config = data_extraction_config
            logging.info('[INFO] Reading Schema YAML file')
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
            self.url = url
            self.offset = offset
            self.num_of_exp = num_of_exp
            self.include_types = includes_type
            self._headers = {'x-api-key': os.environ.get("API_KEY")}
        except Exception as e:
            raise ELNException(e, sys)
    

    def create_dataframe(self):
        col_names = [list(col.keys())[0] for col in self._schema_config['columns']]
        return pd.DataFrame(columns= col_names)
  

    def get_request(self, url):
        return requests.get(url, headers= self._headers)
    
    def get_user_details(self, response_data, columns: list, values: dict):
        try:
            logging.info('[INFO] Extracting User Information from ELN')
            user_url = response_data.get('relationships')['createdBy']['links']['self']
            user_json_data = requests.get(user_url, headers = self._headers)
            user_data = user_json_data.json().get('data')['attributes']
            col_not_found = []
            for col in columns:
                if user_data.get(col):
                    values[col] = [user_data[col]]
                else:
                    col_not_found.append(col)

            logging.info('[INFO] User Information extracted from ELN')
            return values, col_not_found
        
        except Exception as e:
            logging.error("[ERROR] Error occurred while extarcting user information from ELN")
            raise ELNException(e, sys)
        
    def get_data(self, url):

        response = self.get_request(url = url)
        if response.status_code == 200:
            # Successful request
            exp_data = response.json()
            return exp_data
        else:
            # Request was not successful
            print("Request failed with status code:", response.status_code)
        
        

    def get_project_details(self, data, values: dict):
        try:
            exp = self.get_data(url = data['links']['self'])
            logging.info('[INFO] Starting Project information extraction from ELN')
            col_not_found = []
            alternative_names = {'Project Code': 'Project code',
                                'Project Description': 'Project description'}
            for col in self._schema_config['columns']:
                column_name = list(col.keys())[0]

                if column_name in exp['data']['attributes']:
                    values[column_name] = [exp['data']['attributes'][column_name]]
                elif column_name in exp['data']['attributes']['fields']:
                    values[column_name] = [exp['data']['attributes']['fields'][column_name]['value']]
                else:
                    col_not_found.append(column_name)

            for key in alternative_names.keys():
                if key in col_not_found:
                    col_name = alternative_names[key]
                    values[key] = [exp['data']['attributes']['fields'][col_name]['value']]

            return values, col_not_found
        except Exception as e:
            logging.error("[ERROR] Error occurred while extarcting project information from ELN")
            raise ELNException(e, sys)


    def export_data(self) -> pd.DataFrame:
        values= {} 
        dataframe = self.create_dataframe()
        try:
            i = 0
            c = 0
            url = f"{self.url}?page[offset]={self.offset}&page[limit]={self.num_of_exp}&includeTypes={self.include_types}"
            while True: 
                
                response = self.get_request(url = url)
                self_next_urls = response.json().get('links')
                response_data_urls = requests.get(self_next_urls['self'], headers= self._headers)

                content = response_data_urls.json().get('data')
                data_points = len(response_data_urls.json().get('data'))

                for j in range(data_points):
                    if content[j]['type'] == 'entity':

                        values = {}
                        
                        output, col_not_found = self.get_project_details(data= content[j],
                                                                         values= values)
                        # get user details
                        output, col_not_found = self.get_user_details(response_data= content[j], 
                                                                      columns= col_not_found, 
                                                                      values= values)
                        output_df = pd.DataFrame(output)
                        dataframe = pd.concat([dataframe, output_df])

                i += 1
                if 'next' in self_next_urls:
                    self_url = response_data_urls.json().get('links')['self']
                    next_url = response_data_urls.json().get('links')['next']
                    url = next_url
                    save_text_to_csv(url, filename= filename)
                else:
                    break
            
            return dataframe
      
        except Exception as e:
            logging.error("[ERROR] Error occurred in exporting data from ELN")
            raise ELNException(e, sys)
        
    def clean_date_time(self, datetime_str):
        try:
            if datetime_str != 'nan':
                datetime_str = str(datetime_str)
                datetime_obj = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                year = datetime_obj.year
                month = datetime_obj.month
                day = datetime_obj.day
                new_datetime_str = f"{year}/{month}/{day}"
                return new_datetime_str
            return None  # Return None if datetime_str is 'nan' or parsing fails
        except ValueError:
            return None  # Return None if parsing fails
        except Exception as e:
            raise ELNException(e, sys)



    
    def initiate_data_extraction(self):
        try:
            logging.info('[INFO] Initiating Data Extraction..')
            logging.info('[INFO] Data Extracted from ELN')
            dataframe = self.export_data()
            dataframe['createdAt'] = dataframe['createdAt'].apply(self.clean_date_time)
            dataframe['editedAt'] = dataframe['editedAt'].apply(self.clean_date_time)
            dataframe['acceptedAt'] = dataframe['acceptedAt'].apply(self.clean_date_time)
            csv_file = self.data_extraction_config.csv_file
            dir_path = os.path.dirname(self.data_extraction_config.data_extraction_dir)
            os.makedirs(dir_path, exist_ok= True)
            logging.info('[INFO] Saving dataframe to CSV file')
            save_csv(csv_file, dataframe= dataframe)

            return DataExtractionArtifact(data_file= csv_file)

        except  Exception as e:
            raise ELNException(e, sys)
