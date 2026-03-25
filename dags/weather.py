from airflow import DAG
import logging
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
import json

CITIES = {
    "Lviv":  {"lat": 49.8397, "lon": 24.0297},
    "Kyiv":  {"lat": 50.4501, "lon": 30.5234},
    "Odesa": {"lat": 46.4825, "lon": 30.7233},
    "Dnipro": {"lat": 48.4647, "lon": 35.0462},
}

def _process_weather(city_name, task_id, ti):
    info = ti.xcom_pull(task_ids = task_id)
    # For historical endpoint, OpenWeather may return data in "data" list
    # for current in "current"
    if "current" in info:
        weather = info["current"]
    elif "data" in info and len(info["data"]) > 0:
        weather = info["data"][0]
    else:
        raise ValueError(f"Unexpected API response  for {city_name}: {info}")
    city = city_name
    timestamp = weather["dt"]
    temp = weather["temp"]
    humidity = weather["humidity"]
    clouds = weather["clouds"]
    wind_speed  = weather["wind_speed"]
    
    logging.info(f" {city} : {timestamp} : {temp} : {humidity} :{clouds} : {wind_speed}")
    return city, timestamp, temp, humidity, clouds, wind_speed
    

with DAG(
    dag_id="weather_processor",
    start_date=datetime(2026, 3, 23),
    schedule='@daily',
    catchup=True,
    tags=["example", "async", "core"],
) as dag:
    b_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""CREATE TABLE IF NOT EXISTS
                measures
                (city TEXT,
                timestamp TIMESTAMP,
                temp FLOAT,
                humidity INTEGER,
                clouds INTEGER,
                wind_speed FLOAT
                );""",
        )
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn_http",
        endpoint="data/3.0/onecall",
        request_params={
            "lat": 49.8397,
            "lon": 24.0297,
            "appid": Variable.get("WEATHER_API_KEY"),
            "units": "metric"
    },
        )
    process_tasks = []
    for city_name, coords in CITIES.items():
        extract_data = HttpOperator(
            task_id=f"extract_{city_name}",
            http_conn_id="weather_conn_http",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "lat": coords['lat'],
                "lon": coords['lon'],
                "dt": "{{logical_date.int_timestamp }}",
                "appid": Variable.get("WEATHER_API_KEY"),
                "units": "metric",
                },
            method="GET",
            response_check=lambda response: response.status_code == 200,
            response_filter=lambda response: response.json(),
            log_response=True
        )
       
        process_data = PythonOperator(
            task_id=f"process_{city_name}",
            python_callable=_process_weather,
            op_kwargs={
                "city_name": city_name,
                "task_id": f"extract_{city_name}",
            },
            )

        inject_data = SQLExecuteQueryOperator(
            task_id=f"inject_{city_name}",
            conn_id="weather_conn",
            sql=f"""
            INSERT INTO measures (
            city, timestamp, temp, humidity, clouds, wind_speed)
              VALUES(
            '{{{{ti.xcom_pull(task_ids='process_{city_name}')[0]}}}}',
            {{{{ti.xcom_pull(task_ids='process_{city_name}')[1]}}}},
            {{{{ti.xcom_pull(task_ids='process_{city_name}')[2]}}}},
            {{{{ti.xcom_pull(task_ids='process_{city_name}')[3]}}}},
            {{{{ti.xcom_pull(task_ids='process_{city_name}')[4]}}}},
            {{{{ ti.xcom_pull(task_ids="process_{city_name}")[5]}}}}
            );
            """,)
        extract_data >> process_data >> inject_data
        process_tasks.append(extract_data)

b_create >> check_api >> process_tasks

