from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define a simple Python function
def say_hello():
    print("Hello Airflow!")

# Define default arguments for the DAG
default_args = {
    'owner': 'you',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    dag_id='hello_airflow',
    default_args=default_args,
    start_date=datetime(2023, 3, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # Define the task
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=say_hello,
    )

# Set task dependencies (optional, only one task here)
hello_task
