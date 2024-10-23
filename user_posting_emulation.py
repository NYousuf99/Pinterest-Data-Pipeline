import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text,create_engine
import yaml
import pandas as pd 

random.seed(100)


class AWSDBConnector:

    def __init__(self) -> None:
        pass


    def read_db_creds(self,yaml_path='db_creds.yaml') -> dict:
        try:
            with open(yaml_path,'r') as f:
                self.creds = dict(yaml.safe_load(f))
            return self.creds
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Error: Database credentials file '{self.creds}' not found.") from e
        except yaml.YAMLError as e:
            raise ValueError(f"Error: Invalid YAML format in credentials file '{self.creds}'.") from e 


    def create_db_connector(self) -> sqlalchemy.engine.base.Connection:
        creds = self.creds
        USER = creds['USER']
        PASSWORD = creds['PASSWORD']
        HOST = creds['HOST']
        PORT = creds['PORT']
        DATABASE = creds['DATABASE']
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def run_infinite_post_data_loop() -> None:
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        creds = new_connector.read_db_creds()
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            

            print(f"PIN RESULTS = {pin_result}")
            print(f"GEO RESULTS = {geo_result}")
            print(f"USER RESULT = {user_result}")


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


