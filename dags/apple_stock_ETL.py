from airflow import DAG
from airflow.decorators import task
from datetime import datetime,timedelta
import pandas as pd
import json
import requests 
import numpy as np
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy




default_args={'owner':'APPL_stocks',
              'retries':1,
              'depends_on_past':False,
              'retry_delay':timedelta(minutes=5)}

with DAG (dag_id='APPL',start_date=datetime(2025,3,10),schedule_interval='@once',
          default_args=default_args,catchup=False,description='Fetch apple API stocks DAG'
         ) as dag:
    



    create_table=SQLExecuteQueryOperator(
        task_id='creating_table',
        conn_id='airflow_user',
        sql="""
        CREATE TABLE IF NOT EXISTS apple_stock(
        date DATE PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT );"""

    )
    

    @task
    def fetch_data():
        api_key='U9ARU939UUHGTGGM'
        symbol='AAPL'
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response=requests.get(url)
        data=response.json()
        time_series = data.get("Time Series (Daily)", {})
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.reset_index(inplace=True)
        
        # Rename columns to match database
        df.rename(columns={"index": "date",
                           "1. open": "open",
                           "2. high": "high",
                           "3. low": "low",
                           "4. close": "close",
                           "5. volume": "volume"}, inplace=True)
        file_path = "/tmp/appl_stocks_data.csv"
        df.to_csv(file_path, index=False)
        return file_path



    @task
    def clean_data(file_path:str):
        df=pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)
        df["volume"] = df["volume"].astype(int)
        cleaned_data_path = "/tmp/Appl_stock_cleaned.csv"
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)


        df.to_csv(cleaned_data_path,index=False)
        return cleaned_data_path
        


    

    @task
    def upload_data(cleaned_data_path):
        df=pd.read_csv(cleaned_data_path)
        postgres_hook = PostgresHook(postgres_conn_id="airflow_user")
        engine = postgres_hook.get_sqlalchemy_engine()

        # Insert data into the table
        df.to_sql('apple_stock', con=engine, if_exists='append', index=False)


    


    file_path = fetch_data()
    cleaned_path = clean_data(file_path)
    create_table >> file_path >> cleaned_path >> upload_data(cleaned_path)


