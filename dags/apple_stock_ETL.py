from airflow import DAG
from airflow.decorators import task
from datetime import datetime,timedelta
import pandas as pd
import json
import requests as re
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
    

    @task
    def fetch_data():
        api_key='U9ARU939UUHGTGGM'
        symbol='AAPL'
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response=re.get(url)
        data=response.json()
        time_series = data.get("Time Series (Daily)", {})
        df=pd.DataFrame(time_series).T
        df.to_csv('appl_stocks_data')



    @task
    def clean_data():
        data=pd.read_csv('appl_stocks_data')
        df=pd.DataFrame(data)
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        cleaned_data='Appl_stock.csv'
        df.to_csv(cleaned_data)
        


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
    def upload_data(cleaned_data):
        df=pd.read_csv(cleaned_data)
        postgres_hook = PostgresHook(postgres_conn_id="airflow_user")
        engine = postgres_hook.get_sqlalchemy_engine()

        # Insert data into the table
        df.to_sql('apple_stock', con=engine, if_exists='append', index=False)


    


    create_table() >> fetch_data() >> clean_data() >> upload_data()


