from airflow import DAG
import requests
import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_data():
    api_key = "U9ARU939UUHGTGGM"
    symbol = "AAPL"
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    response = requests.get(url)
    data = response.json()
    time_series = data.get("Time Series (Daily)", {})

    # Convert the time series data into a DataFrame
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.sort_values(['date'])

    # Save the DataFrame to a CSV file
    df.to_csv('AAPL_stock_data.csv')


def clean_data():
    df=pd.read_csv('AAPL_stock_data.csv')
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.to_csv('cleaned_AAPL_stocks.csv')


def database_upload():
    hook = PostgresHook(postgres_conn_id='airflow_user')
    conn = hook.get_conn()
    cursor = conn.cursor()

    df = pd.read_csv('cleaned_AAPL_stocks.csv')
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO AAPL_stock_data (date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (date) DO NOTHING;",
            (row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    



with DAG ('fetch_dag',start_date=datetime (2025,3,6),
          schedule_interval='@once',catchup=False,retries=1,description='A basic DAG to fetch stock data of apple using alphavantage and uploading it to PostgresSQL'
) as dag:
    

    create_table=SQLExecuteQueryOperator(
        task_id='create_table',
        postgres_conn_id='airflow_user',
        sql=""" CREATE TABLE IF NOT EXISTS AAPL_stock_data(
        date DATE PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
        );
        """
    )



    fetch_task=PythonOperator(
        task_id='api_call',
        python_callable=fetch_data
    )


    clean_task=PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    database_task=PythonOperator(
        task_id='data_upload',
        python_callable=database_upload

    )


    create_table>>fetch_task>>clean_task>>database_task