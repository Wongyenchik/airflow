from airflow import DAG
from datetime import timedelta, datetime, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return round(temp_in_fahrenheit, 3)

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_a.tsk_extract_houston_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    timestamp = data['dt'] + data['timezone']
    time_of_record = datetime.fromtimestamp(timestamp)
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])
    # sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])


    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "time_of_record": time_of_record,
                        "sunrise_local_time)":sunrise_time,
                        "sunset_local_time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    dt_string = 'current_weather_data'
    df_data.to_csv(f"s3://scott-wong-airflow/{dt_string}.csv", index=False)

def load_weather():
    # Use the PostgresOperator to load the CSV data from S3 into the PostgreSQL table
    hook = PostgresHook(postgres_conn_id='aws_rds')
    sql = """
    SELECT aws_s3.table_import_from_s3(
        'weather_data',
        '',
        '(format csv, HEADER true)',
        'scott-wong-airflow',
        'current_weather_data.csv',
        'ap-southeast-2'
    );
    """
    hook.run(sql)

def save_joined_data_s3():
    # Fetch data from the combined_weather_data table
    hook = PostgresHook(postgres_conn_id='aws_rds')
    sql = """
    SELECT * FROM aws_s3.query_export_to_s3(
        'SELECT * FROM combined_weather_data1',
        aws_commons.create_s3_uri(
            'scott-wong-airflow',
            'combined_weather_data1.csv',
            'ap-southeast-2'
        ),
        options := 'format csv, delimiter $$,$$'
    );
    """
    hook.run(sql)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('apis3postgres',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = DummyOperator(
            task_id = 'tsk_start_pipeline'
        )

        join_data = PostgresOperator(
                task_id='task_join_data',
                postgres_conn_id = "aws_rds",
                    sql='''
                        CREATE TABLE IF NOT EXISTS combined_weather_data1 AS
                        SELECT 
                            w.City,                    
                            w.description,
                            w.temperature_farenheit,
                            w.feels_like_farenheit,
                            w.minimun_temp_farenheit,
                            w.maximum_temp_farenheit,
                            w.pressure,
                            w.humidity,
                            w.wind_speed,
                            w.time_of_record,
                            w.sunrise_local_time,
                            w.sunset_local_time
                        FROM weather_data w
                        INNER JOIN city_look_up c
                            ON w.City = c.city;
                        '''
            )
        
        load_joined_data = PythonOperator(
            task_id= 'task_load_joined_data',
            python_callable=save_joined_data_s3
            )
        
        end_pipeline = DummyOperator(
            task_id = 'task_end_pipeline'
        )



        with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_S3_and_weatherapi") as group_A:
            create_table_1 = PostgresOperator(
                task_id='tsk_create_table_1',
                postgres_conn_id = "aws_rds",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS city_look_up (
                    city TEXT NOT NULL,
                    state TEXT NOT NULL,
                    census_2020 numeric NOT NULL,
                    land_Area_sq_mile_2020 numeric NOT NULL                    
                );
                '''
            )

            truncate_table = PostgresOperator(
                task_id='tsk_truncate_table',
                postgres_conn_id = "aws_rds",
                sql= ''' TRUNCATE TABLE city_look_up;
                    '''
            )

            uploadS3_to_postgres  = PostgresOperator(
                task_id = "tsk_uploadS3_to_postgres",
                postgres_conn_id = "aws_rds",
                sql = "SELECT aws_s3.table_import_from_s3('city_look_up', '', '(format csv, DELIMITER '','', HEADER true)', 'scott-wong-airflow', 'us_city.csv', 'ap-southeast-2');"
            )

            create_table_2 = PostgresOperator(
                task_id='tsk_create_table_2',
                postgres_conn_id = "aws_rds",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    city TEXT,
                    description TEXT,
                    temperature_farenheit NUMERIC,
                    feels_like_farenheit NUMERIC,
                    minimun_temp_farenheit NUMERIC,
                    maximum_temp_farenheit NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
                );
                '''
            )

            is_houston_weather_api_ready = HttpSensor(
                task_id ='tsk_is_houston_weather_api_ready',
                http_conn_id='weathermap_api',
                endpoint='/data/2.5/weather?q=Chicago&appid=2e5576f8077d71707ed1837401e4daac'
            )

            extract_houston_weather_data = SimpleHttpOperator(
                task_id = 'tsk_extract_houston_weather_data',
                http_conn_id = 'weathermap_api',
                endpoint='/data/2.5/weather?q=Chicago&appid=2e5576f8077d71707ed1837401e4daac',
                method = 'GET',
                response_filter= lambda r: json.loads(r.text),
                log_response=True
            )

            transform_load_houston_weather_data = PythonOperator(
                task_id= 'transform_load_houston_weather_data',
                python_callable=transform_load_data
            )

            load_weather_data = PythonOperator(
                task_id= 'tsk_load_weather_data',
                python_callable=load_weather
            )


        
            
            create_table_1 >> truncate_table >> uploadS3_to_postgres
            create_table_2 >> is_houston_weather_api_ready >> extract_houston_weather_data >> transform_load_houston_weather_data >> load_weather_data
        start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline



