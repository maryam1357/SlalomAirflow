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
    'owner': 'Na Yu',
    'start_date': datetime(2020, 3, 27, 10, 45),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

#pipeline schedule
pacific_time_three_am = '0 11 * * *'

dag = DAG(
    'airport_auto_ny',
    default_args=default_args,
    schedule_interval=pacific_time_three_am
)

BQ_CONN_ID = "my_gcp_conn_ny"
BQ_PROJECT = "eastern-map-272014"
BQ_DATASET = "airport_infor"

with dag:

    #load airport csv from gcs to bq airport_auto
    load_airport_csv = GoogleCloudStorageToBigQueryOperator(
        task_id='airport_gcs_to_bquery',
        bucket='us-central1-example-envrion-e37d4b20-bucket', 
        source_objects=['data/airport-codes_csv.csv'],
        destination_project_dataset_table='airport_infor.Airport_Code',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,        
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    #DDL for airport_na_auto
    airport_na_create = BigQueryOperator(
        task_id='airport_auto_create',
        sql='''
        #standardSQL
        CREATE TABLE IF NOT EXISTS
        `eastern-map-272014.airport_infor.airport_na_auto`(
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
        DELETE FROM airport_infor.airport_na_auto WHERE TRUE;
        INSERT INTO airport_infor.airport_na_auto
        SELECT * FROM airport_infor.Airport_Code WHERE
            type != 'closed'
            AND continent = 'NA';
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID       
    )

    load_airport_csv >> airport_na_insert
    airport_na_create >> airport_na_insert

    #linear chaining(not as efficient): chain(load_airport_csv, airport_na_create, airport_na_insert)

    