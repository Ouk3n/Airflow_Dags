from airflow import DAG
from airflow.decorators import task
from datetime import datetime,timedelta






with DAG (dag_id='Fetch_api_dag',start_date=datetime(2025,4,1)
          ,schedule_interval='@once',description='fetch api stock data using @task decorator',
          catchup=False) as dag:
    


    @task
    def fetch_data():
        print('fetching data...')
        return "fetched data"


    @task
    def clean_data():
         print('cleaning data..')
         return "data cleaned"


    @task
    def loading_data():
        print('loading data...')
        return "data loaded"
    


    fetch_data() >> clean_data() >>loading_data()
    

