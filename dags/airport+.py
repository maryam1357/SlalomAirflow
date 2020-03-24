from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow import configuration as conf
from airflow import models
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'Chester Tai',
    'start_date': datetime(2020, 3, 23, 10, 45),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

#pipeline schedule
pacific_time_three_am = '0 11 * * *'

dag = DAG(
    'airport',
    default_args=default_args,
    schedule_interval=pacific_time_three_am
)

BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "practice-271500"
BQ_DATASET = "sandbox"


with dag:
    
    #load airport csv from gcs to bq
    load_airport_csv = GoogleCloudStorageToBigQueryOperator(
        task_id='airport_gcs_to_bq',
        bucket='us-central1-slalom-practice-eef43966-bucket',
        source_objects=['data/raw_data/airport-codes_csv.csv'],
        destination_project_dataset_table='sandbox.airport',
        source_format='CSV',
        schema_fields=[
            {'name': 'ident', 'type': 'STRING'},
            {'name': 'type', 'type': 'STRING'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'elevation_ft', 'type': 'INT64'},
            {'name': 'continent', 'type': 'STRING'},
            {'name': 'iso_country', 'type': 'STRING'},
            {'name': 'iso_region', 'type': 'STRING'},
            {'name': 'municipality', 'type': 'STRING'},
            {'name': 'gps_code', 'type': 'STRING'},
            {'name': 'iata_code', 'type': 'STRING'},
            {'name': 'local_code', 'type': 'STRING'},
            {'name': 'coordinates', 'type': 'STRING'},
        ],        
        write_disposition='WRITE_TRUNCATE'
    )

    #DDL for airport
    airport_na_create = BigQueryOperator(
        task_id='airport_na_create',
        sql='''
        #standardSQL
        DROP TABLE IF EXISTS
        `practice-271500.sandbox.airport_na`;
        CREATE TABLE IF NOT EXISTS
        `practice-271500.sandbox.airport_na`(
            ident STRING,
            type STRING,
            name STRING,
            elevation_ft INT64,
            continent STRING,
            iso_country STRING,
            iso_region STRING,
            municipality STRING,
            gps_code STRING,
            iata_code STRING,
            local_code STRING,
            coordinates STRING)
        ''',
        use_legacy_sql = False,
        bigquery_con_id=BQ_CONN_ID
    )

    # illustration - creating a derivative table from an existing BQ table. in the real world we might do this directly from the csv, depending on cost effectiveness.
    airport_na_insert = BigQueryOperator(
        task_id='airport_na_insert',
        sql='''
        #standardSQL
        DELETE FROM sandbox.airport_na WHERE TRUE;
        INSERT INTO sandbox.airport_na
        SELECT * FROM sandbox.airport WHERE
            type != 'closed'
            AND continent = 'NA';
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID       
    )

chain(load_airport_csv, airport_na_create, airport_na_insert)

    