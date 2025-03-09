from airflow import DAG
from airflow.decorators import task
from datetime import datetime,timedelta
import pandas as pd
import json
import requests as re
import numpy as np
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



default_args={'owner':'APPL_stocks',
              'retries':1,
              'depends_on_past':False,
              'retry_delay':timedelta(minutes=5)}

with DAG (dag_id='APPL',start_date=datetime(2025,3,10),schedule_interval='@once',
          default_args=default_args,catchup=False,description='Fetch apple API stocks DAG'
         ) as dag:
    

    @task
    def fetch_data():
        api_key='U9ARU939UUHGTGGM'
        symbol='AAPL'
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response=re.get(url)
        data=response.json()
        file_path = '/tmp/Appl_stock_data.json'
        with open(file_path,'w') as f:
             json.dump(data, f)
        return file_path


    @task
    def clean_data():
        data=pd.read_json('/tmp/Appl_stock_data.json')
        df=pd.DataFrame(data).T
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        cleaned_data='/tmp/Appl_stock.csv'
        df.to_csv(cleaned_data)
        return cleaned_data


    fetch_data()>>clean_data()


