from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'Maryam Veisi',
    'start_date': datetime(2020, 3, 30, 11, 00),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

#pipeline schedule
pacific_time_three_am = '*/10 * * * *'

dag = DAG(
    'demography_auto',
    default_args=default_args,
    schedule_interval=pacific_time_three_am
)

BQ_CONN_ID = "my_gcp_conn"
BQ_PROJECT = "airflow-slalom"
BQ_DATASET = "sandbox"

with dag:

    #load airport csv from gcs to database Airport table
    load_airport_csv = GoogleCloudStorageToBigQueryOperator(
        task_id='airport_gcs_to_bq',
        bucket='us-central1-airflow-slalom--1808e202-bucket',
        source_objects=['data/airport-codes_csv.csv'],
        destination_project_dataset_table='sandbox.Airport',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,        
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

        #load demography csv from gcs to database
    load_demography_csv = GoogleCloudStorageToBigQueryOperator(
        task_id='demography_gcs_to_bq',
        bucket='us-central1-airflow-slalom--1808e202-bucket',
        source_objects=['data/us-cities-demographics.csv'],
        destination_project_dataset_table='sandbox.city_demographic',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=1,        
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    #create city table drieved from Airpot table
    city_create_insert = BigQueryOperator(
        task_id='city_auto_create_insert',
        sql='''
        #standardSQL
        CREATE OR REPLACE TABLE airflow-slalom.sandbox.city
        AS
        SELECT State, City FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY State, City) row_number FROM 
        (Select State_Code as State, City from airflow-slalom.sandbox.city_demographic 
        union all 
        Select SPLIT(iso_region, '-')[OFFSET(1)] State ,municipality as City from airflow-slalom.sandbox.Airport 
        where iso_country = 'US') )WHERE row_number = 1 and State is not null and City is not null)
        ''',
        use_legacy_sql = False,
        bigquery_con_id=BQ_CONN_ID
    )

    # demo table joined with city table 
    demography_create_insert = BigQueryOperator(
        task_id='demography_auto_create_insert',
        sql='''
        #standardSQL
        CREATE OR REPLACE TABLE airflow-slalom.sandbox.demography_auto
        AS
        select * except(city_name, State_Code) from (select * from airflow-slalom.sandbox.city as city join 
        (select Male_Population, Female_Population, Total_Population, Number_of_Veterans, Foreign_born, 
        Average_Household_Size, State_Code, Race, Median_Age, City as city_name 
        from airflow-slalom.sandbox.city_demographic)as demo on demo.city_name = city.City and 
        demo.State_Code = city.State);
        ''',
        use_legacy_sql=False,
        bigquery_conn_id=BQ_CONN_ID       
    )


    load_airport_csv >> city_create_insert
    load_demography_csv >> city_create_insert
    city_create_insert >> demography_create_insert




  



