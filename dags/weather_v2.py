from airflow import DAG
import logging
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.sdk import TaskGroup
import sqlite3
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TriggerRule

CITIES = {
    "Lviv":  {"lat": 49.8397, "lon": 24.0297},
    "Kyiv":  {"lat": 50.4501, "lon": 30.5234},
    "Odesa": {"lat": 46.4825, "lon": 30.7233},
    "Dnipro": {"lat": 48.4647, "lon": 35.0462},
}
WIND_THRESHOLD = 10.0

def _choose_load_path(city_name, transform_task_id, threshold, ti):
    row = ti.xcom_pull(task_ids=transform_task_id)

    if row is None:
        raise ValueError(f"No XCom found for task_id={transform_task_id}")

    wind_speed = row["wind_speed"]

    if wind_speed >= threshold:
        logging.info(
            "Wind speed %.2f in %s is above threshold %.2f -> alert path",
            wind_speed, city_name, threshold
        )
        return f"process_city.{city_name}.send_alert_{city_name}"

    logging.info(
        "Wind speed %.2f in %s is below threshold %.2f -> normal path",
        wind_speed, city_name, threshold
    )
    return f"process_city.{city_name}.normal_load_{city_name}"


def _send_alert(city_name, transform_task_id, threshold, ti):
    row = ti.xcom_pull(task_ids=transform_task_id)

    if row is None:
        raise ValueError(f"No XCom found for task_id={transform_task_id}")

    logging.warning(
        "ALERT: %s wind speed %.2f crossed threshold %.2f",
        city_name,
        row["wind_speed"],
        threshold,
    )

def _transform_weather(city_name, fetch_task_id, ti):
    info = ti.xcom_pull(task_ids = fetch_task_id)
    # For historical endpoint, OpenWeather may return data in "data" list
    # for current in "current"
    if info is None:
        raise ValueError(f"No XCom found for city={city_name}, task_id={fetch_task_id}")

    if "current" in info:
        weather = info["current"]
    elif "data" in info and len(info["data"]) > 0:
        weather = info["data"][0]
    else:
        raise ValueError(f"Unexpected API response  for {city_name}: {info}")
    row = {
        'city': city_name,
        'timestamp':  weather["dt"],
        'temp': weather["temp"],
        'humidity': weather["humidity"],
        'clouds': weather["clouds"],
        'wind_speed': weather["wind_speed"],
    }
    
    logging.info("Transformed row: %s", row)
    return row

def _load_weather(transform_task_id, ti):
    row = ti.xcom_pull(task_ids=transform_task_id)

    if row is None:
        raise ValueError(f"No XCom found for task_id={transform_task_id}")

    conn = sqlite3.connect("weather.db")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO measures (
                city, timestamp, temp, humidity, clouds, wind_speed
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["city"],
                row["timestamp"],
                row["temp"],
                row["humidity"],
                row["clouds"],
                row["wind_speed"],
            ),
        )
        conn.commit()
    finally:
        conn.close()

with DAG(
    dag_id="weather_processor_v2",
    start_date=datetime(2026, 3, 23),
    schedule='@daily',
    catchup=False,
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
        endpoint="data/3.0/onecall/timemachine",
        request_params={
            "lat": 49.8397,
            "lon": 24.0297,
            "dt": "{{logical_date.int_timestamp }}",
            "appid": Variable.get("WEATHER_API_KEY"),
            "units": "metric"
    },
        )
    
    def city_pipeline(city_name, coords):
        with TaskGroup(group_id=city_name) as task_group:
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
        
            transform = PythonOperator(
                task_id=f"transform_{city_name}",
                python_callable=_transform_weather,
                op_kwargs={
                    "city_name": city_name,
                    "fetch_task_id": f"process_city.{city_name}.extract_{city_name}",
                },
                )
            branch = BranchPythonOperator(
                task_id='branch_on_wind',
                python_callable=_choose_load_path,
                op_kwargs={
                    'city_name': city_name,
                    'transform_task_id':f'process_city.{city_name}.transform_{city_name}',
                    'threshold': WIND_THRESHOLD,    
                },
            )
            send_alert = PythonOperator(
                task_id=f"send_alert_{city_name}",
                python_callable=_send_alert,
                op_kwargs={
                    "city_name": city_name,
                    "transform_task_id": f"process_city.{city_name}.transform_{city_name}",
                    "threshold": WIND_THRESHOLD,
            },
        )

            normal_load = PythonOperator(
                task_id=f"normal_load_{city_name}",
                python_callable=_load_weather,
                 op_kwargs={
                    "transform_task_id": f"process_city.{city_name}.transform_{city_name}",
                },
            )

            alert_load = PythonOperator(
                task_id=f"alert_load_{city_name}",
                python_callable=_load_weather,
                op_kwargs={
                    "transform_task_id": f"process_city.{city_name}.transform_{city_name}",
                },
        )
            done = EmptyOperator(
                task_id="done",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
            extract_data >> transform >> branch
            branch >> normal_load >> done
            branch >> send_alert >> alert_load >> done
        return task_group

    with TaskGroup(group_id="process_city") as process_city:
        for city_name, coords in CITIES.items():
            city_pipeline(city_name, coords)
    
        

b_create >> check_api >> process_city

