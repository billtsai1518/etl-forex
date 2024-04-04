from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
import json
import pandas as pd

# Default settings for all the dags in the pipeline
default_args = {
    "owner": "billtsai1518", 
    "start_date" : datetime(2024, 1, 1, 14, 00),
    "retries" : 1,
    "retry_delay": timedelta(minutes=5)
}

def _process_data(ti):

    data = ti.xcom_pull(task_ids = 'extract_data') # Extract the json object from XCom
    data = data['response'] # Extract the symbols and the rates in a dictionary 
    df = pd.DataFrame(data)
    processed_data = df.set_index('s', inplace=False).T.loc['c']

    # Export DataFrame to CSV
    processed_data.to_csv('/tmp/processed_data.csv', header=False)


def _store_data():
    '''
    This function uses the Postgres Hook to copy users from processed_data.csv
    and into the table
    
    '''
    # Connect to the Postgres connection
    hook = PostgresHook(postgres_conn_id = 'postgres')

    # Insert the data from the CSV file into the postgres database
    hook.copy_expert(
        sql = "COPY rates FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_data.csv'
    )


with DAG('forex_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:


    # Dag #1 - Check if the API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        method='GET',
        http_conn_id='forex_api',
        endpoint='',
        response_check= lambda response: 'Successfully' in response.text,
        poke_interval = 5
    )

    # Dag #2 - Create a table
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres',
        sql='''
            drop table if exists rates;
            create table rates(
                symbol text not null,
                rate float not null
            );
        '''
    )

    # DAG #3 - Extract Data
    extract_data = SimpleHttpOperator(
            task_id = 'extract_data',
            http_conn_id='forex_api',
            method='GET',
            endpoint='',
            response_filter=lambda response: json.loads(response.text),
            log_response=True
    )

    # Dag #4 - Transform data
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable=_process_data

    )

    # Dag #5 - Load data
    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=_store_data

    )

    # Dependencies
    is_api_available >> create_table >> extract_data >> transform_data >> load_data
