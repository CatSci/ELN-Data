import os, sys
import psycopg2
from elndata.entity.config_entity import DatabaseConfig
from elndata.entity.artifact_entity import DataExtractionArtifact
from elndata.constant.data import SCHEMA_FILE_PATH
from elndata.utils.main import read_yaml_file
from elndata.logger import logging
from elndata.exception import ELNException
import pandas as pd
import csv, math
import numpy as np




class Database:
    def __init__(self, 
                 database_config: DatabaseConfig):
        
        try:
            self.database_config = database_config
            self.conn = psycopg2.connect(user= self.database_config.user_name,
                                        database = self.database_config.database,
                                        password= self.database_config.password,
                                        host= self.database_config.host,
                                        port= self.database_config.port)
            
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()

        except Exception as e:
            raise ELNException(e, sys)
        
    def column_names(self, file_path):
        try:
            self._schema_config = read_yaml_file(file_path)
            columns = []
            for col in self._schema_config['columns']:
                columns.append(list(col.keys())[0])
            
            return columns
        
        except Exception as e:
            raise ELNException(e, sys)

    
    def _check_databse_exists(self)-> bool:
        """_summary_

        Raises:
            ELNException: _description_

        Returns:
            bool: _description_
        """
        try:
            self.cursor.execute(f"SELECT datname FROM pg_database WHERE datname='{self.database_config.database_name}';")

            # check if the database exists
            database_exists = bool(self.cursor.rowcount)
            
            return database_exists
        except Exception as e:
            raise ELNException(e, sys)
    
    def _check_table_exists(self, table_name: str)-> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        self.cursor.execute(f"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' AND tablename='{table_name}')")
        return self.cursor.fetchone()[0]
    
    def _check_table_empty(self, table_name:str)-> bool:
        """_summary_

        Args:
            table_name (str): name of the table to check
        
        Returns:
            bool: True if equals to 0 means table is empty
        """
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return self.cursor.fetchone()[0] == 0
    
    def _create_database(self):
        """_summary_

        Returns:
            None
        """
        try:
            logging.info("[INFO] Creating Database")
            database_exist = self._check_databse_exists()
            if not database_exist:
                create_db = f"CREATE database {self.database_config.database_name};" 
                self.cursor.execute(create_db)
            self.conn = psycopg2.connect(user=self.database_config.user_name,
                                        database=self.database_config.database_name,
                                        password=self.database_config.password,
                                        host=self.database_config.host,
                                        port=self.database_config.port)
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            logging.info("[INFO] Database created Successfully !!!")
        except Exception as e:
            logging.error("[ERROR] Error occurred in creating Database")
            raise ELNException(e, sys)
    
    
    def _create_table(self, columns_list: list, primary_key = 'eid', table_name = 'info'):
        """
        Creating table into database

        Returns:
            None
        """
        try:
            logging.info("[INFO] Creating Table")
            if not self._check_table_exists(table_name= table_name):
                columns = columns_list
                columns_str = ", ".join([f'"{col}" VARCHAR(255)' for col in columns])
                
                create_table_query = f"CREATE TABLE {table_name} ({columns_str}, PRIMARY KEY ({primary_key}))"
                self.cursor.execute(create_table_query)
                print(f'table created')
            else:
                print(f'Table "{table_name}" already exists in database')
            logging.info("[INFO] Table created Successfully !!!")
        except Exception as e:
            logging.error("[ERROR] Error occurred in creating table")
            raise ELNException(e, sys)

    def insert_data(self, table_name:str, extracted_data_file_path:str):
        """
        Inserting data into table

        Returns:
            None
        """
        try:
            logging.info("[INFO] Adding Data into Table")
            if self._check_table_empty(table_name= table_name):
                df = pd.read_csv(extracted_data_file_path)
                col_names = ['"{}"'.format(c) for c in df.columns]
                query = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, ','.join(col_names), ','.join(['%s'] * len(df.columns)))
                data = [tuple(row) for row in df.to_numpy()]
                self.cursor.executemany(query, data)
                print(f'{len(data)} rows inserted into {table_name} table')
            else:
                print(f'Data Already exists in "{table_name}" table')
                logging.info("[INFO] Data added into table successfully !!!")
        except Exception as e:
            logging.error("[ERROR] Error occurred while adding data into table")
            raise ELNException(e, sys)
    
        
    
    def add_delete(self, extracted_data_file_path:str, table_name = 'info'):
        try:
            logging.info("[INFO] Updating Data into table")
            df = pd.read_csv(extracted_data_file_path)
            df.fillna(float('NaN'), inplace=True)
            csv_columns = df.columns
    
            self.cursor.execute(f"SELECT * FROM {table_name}")
            db_data = self.cursor.fetchall()


            # if data point is not in csv but present in sql database then we need to delete the rows from database
            for db_row in db_data:
                db_eid = db_row[0]
                if db_eid not in df['eid'].values:
                    logging.info("[INFO] Deleting Data from table")
                    delete_query = f"DELETE FROM {table_name} WHERE \"eid\" = %s"
                    self.cursor.execute(delete_query, (str(db_eid),))
                    logging.info("[INFO] Deleted Data from table successfully !!!")


            # to update rows in sql database if some values in csv file changed
            # to add rows in database if new rows are added in csv file 
            for index, row in df.iterrows():
                matching_row = next((db_row for db_row in db_data if db_row[0] == row['eid']), None)
                
                if matching_row:
                    update_query = f"UPDATE {table_name} SET "
                    update_values = []
                    values = []
                    for field in csv_columns:
                        db_value = matching_row[list(csv_columns).index(field)]
                        df_value = row[field]
                        if (db_value == df_value) or str(db_value).lower() == 'nan' and str(df_value).lower() == 'nan':
                            continue
                        else:
                            update_values.append(f'"{field}" = %s')
                            values.append(str(df_value))
            
                        print(f"values to update {update_values}")
                    if update_values:
                        logging.info("[INFO] Updating some more data into table")
                        update_query += ", ".join(update_values)
                        update_query += f" WHERE \"eid\" = %s"
                        values.append(str(row['eid']))
                        self.cursor.execute(update_query, values)
                        logging.info("[INFO] Updating some more data into table was successful !!!")
                
                else:
                    logging.info("[INFO] Adding some more data into table")
                    col_names = ['"{}"'.format(c) for c in csv_columns]
                    insert_query = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, ','.join(col_names), ','.join(['%s'] * len(csv_columns)))
                    values = [str(row[field]) for field in csv_columns]
                    self.cursor.execute(insert_query, tuple(values))
                    logging.info("[INFO] Adding some more data into table was successful !!!")

        except Exception as e:
            logging.error("[Error]  Error occurred while updating Data into table")
            raise ELNException(e, sys)

    def execute(self, extracted_data_file_path):
        """_summary_

        Returns:
            None
        """
        try:
            logging.info("[INFO] Pushing Data into database has started")
            self._create_database()
            self._create_table(columns_list= self.column_names(SCHEMA_FILE_PATH), primary_key= 'eid' ,table_name= 'info')
            self.insert_data(table_name= 'info', extracted_data_file_path= extracted_data_file_path)
            self.add_delete(table_name = 'info', extracted_data_file_path= extracted_data_file_path)
            logging.info("[INFO] Pushing Data into database is successfull !!!")

        except Exception as e:
            logging.error("[ERROR] Error occurred while pushing Data into database")
            raise ELNException(e, sys)


# if __name__ == "__main__":
#     d = Database(database_config= DatabaseConfig())
#     d.execute()


