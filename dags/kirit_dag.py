from airflow import DAG
import datetime as dt
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


import pandas as pd

def cull_table(input_file, output_file):
    ''' 
    Basic transformation
    Renameing column names
    Removed unwanted columns from original file
    Remove nans and unwanted lat/long rows
    #To-Do: Reshape to optimize query times. (Create column for times with each row for a new day 
    instead of column per time. )
    '''
    dataframe = pd.read_csv(input_file).fillna('')
    dataframe = dataframe.rename(columns = {'Province/State' : 'province_state', 'Country/Region' : 'country_region', 'Lat' : 'lat', 'Long' :'long'})
    
    dataframe['lat_long'] = dataframe['lat'].map(str) + ', ' + dataframe['long'].map(str)
    dataframe = dataframe.drop(['province_state', 'lat', 'long'], axis = 1)
    
    columns = list(dataframe.columns)
    columns = [columns[0]] + [columns[-1]] + columns[2:]
    columns.pop()
    dataframe = dataframe.reindex(columns = columns)

    # Remove cruise ships (0, 0)
    indices = dataframe.loc[dataframe['lat_long'] == '0.0, 0.0'].index
    dataframe = dataframe.drop(indices, axis = 0)

    dataframe.to_csv(output_file, index=False)

file_paths =\
[
    'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
    'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv',
    'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
]

project_root = '/home/airflow/gcs/data/'
raw_data_folder_path = project_root + 'raw_data'
clean_data_folder_path = project_root + 'clean_data'

bq_con_id = 'bigquery_default'
gcs_con_id = 'bigquery_default'

default_args =\
{
    'owner': 'me',
    'start_date': dt.datetime(2020, 3, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG\
(
    'corona_analysis',
    default_args=default_args, 
    schedule_interval='0 * * * *',
)

download_confirmed_global = BashOperator\
(
    task_id='download_confirmed_global', 
    bash_command='wget -O {{ params.ROOT }}/global_cases.csv {{ params.PATH }} ', 
    params={'ROOT' : raw_data_folder_path, 'PATH' : file_paths[0]},
    dag=dag
)

download_recovered_global = BashOperator\
(
    task_id='download_recovered_global', 
    bash_command='wget -O {{ params.ROOT }}/global_recovered.csv {{ params.PATH }} ', 
    params={'ROOT' : raw_data_folder_path, 'PATH' : file_paths[1]},
    dag=dag
)

download_deaths_global = BashOperator\
(
    task_id='download_deaths_global', 
    bash_command='wget -O {{ params.ROOT }}/global_deaths.csv {{ params.PATH }} ', 
    params={'ROOT' : raw_data_folder_path, 'PATH' : file_paths[2]},
    dag=dag
)

transform_global_cases = PythonOperator\
(
    task_id='transform_global_cases',
    python_callable=cull_table, 
    op_kwargs={'input_file' : raw_data_folder_path + '/global_cases.csv', 'output_file' : clean_data_folder_path + '/global_cases_clean.csv' },
    dag=dag
)

transform_global_recovered = PythonOperator\
(
    task_id='transform_global_recovered',
    python_callable=cull_table, 
    op_kwargs={'input_file' : raw_data_folder_path + '/global_recovered.csv', 'output_file' : clean_data_folder_path + '/' + 'global_recovered_clean.csv' },
    dag=dag
)

transform_global_deaths = PythonOperator\
(
    task_id='transform_global_deaths',
    python_callable=cull_table, 
    op_kwargs={'input_file' : raw_data_folder_path + '/global_deaths.csv', 'output_file' : clean_data_folder_path + '/' + 'global_deaths_clean.csv' },
    dag=dag
)

load_global_cases = GoogleCloudStorageToBigQueryOperator(
    task_id='load_global_cases',
    bucket='us-west3-corona-environment-e293b7ec-bucket',
    source_objects=['data/clean_data/global_cases_clean.csv'],
    bigquery_conn_id=bq_con_id,
    google_cloud_storage_conn_id=gcs_con_id,
    destination_project_dataset_table='covid19.global_cases',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,        
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag
)

load_global_recovered = GoogleCloudStorageToBigQueryOperator(
    task_id='load_global_recovered',
    bucket='us-west3-corona-environment-e293b7ec-bucket',
    source_objects=['data/clean_data/global_recovered_clean.csv'],
    bigquery_conn_id=bq_con_id,
    google_cloud_storage_conn_id=gcs_con_id,
    destination_project_dataset_table='covid19.global_recovered',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,        
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag
)

load_global_deaths = GoogleCloudStorageToBigQueryOperator(
    task_id='load_global_deaths',
    bucket='us-west3-corona-environment-e293b7ec-bucket',
    source_objects=['data/clean_data/global_deaths_clean.csv'],
    bigquery_conn_id=bq_con_id,
    google_cloud_storage_conn_id=gcs_con_id,
    destination_project_dataset_table='covid19.global_deaths',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,        
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag
)


download_confirmed_global >> transform_global_cases >> load_global_cases
download_recovered_global >> transform_global_recovered >> load_global_recovered
download_deaths_global >> transform_global_deaths >> load_global_deaths

