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
    'airport_auto',
    default_args=default_args,
    schedule_interval=pacific_time_three_am
)

BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "practice-271500"
BQ_DATASET = "sandbox"

with dag:

    #load airport csv from gcs to bq airport_auto
    load_airport_csv = GoogleCloudStorageToBigQueryOperator(
        task_id='airport_gcs_to_bq',
        bucket='us-central1-slalom-practice-eef43966-bucket',
        source_objects=['data/raw_data/airport-codes_csv.csv'],
        destination_project_dataset_table='sandbox.airport_auto',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,        
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    #DDL for airport_na_auto
    airport_na_create = BigQueryOperator(
        task_id='airport_na_auto_create',
        sql='''
        #standardSQL
        CREATE TABLE IF NOT EXISTS
        `practice-271500.sandbox.airport_na_auto`(
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
        task_id='airport_na_auto_insert',
        sql='''
        #standardSQL
        DELETE FROM sandbox.airport_na_auto WHERE TRUE;
        INSERT INTO sandbox.airport_na_auto
        SELECT * FROM sandbox.airport_auto WHERE
            type != 'closed'
            AND continent = 'NA';
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID       
    )

    load_airport_csv >> airport_na_insert
    airport_na_create >> airport_na_insert

    #linear chaining(not as efficient): chain(load_airport_csv, airport_na_create, airport_na_insert)

    