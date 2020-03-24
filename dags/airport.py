from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow import configuration as conf
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


default_args = {
    'owner': 'Chester Tai',
    'start_date': datetime(2020, 3, 23, 10, 45),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
pacific_time_three_am = '0 12 * * *'

dag = DAG(
    'airport',
    default_args=default_args,
    schedule_interval=pacific_time_three_am
)

BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "practice-271500"
BQ_DATASET = "sandbox"


with dag:
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

chain(airport_na_create, airport_na_insert)

    