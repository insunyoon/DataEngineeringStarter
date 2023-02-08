# full request

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def create_table(cur, schema, table): 
    sql = """CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date primary key,
    temp float,
    min_temp float,
    max_temp float,
    created_date timestamp default GETDATE()
    );""".format(schema=schema, table=table)
    cur.execute(sql)
#    cur.execute("COMMIT;") 없애고 시도해보기
    return

def extract(**context):
    link = context["params"]["url"]
    task_instance = context["task_instance"]
    execution_date = context["execution_date"]
    
    logging.info(execution_date)
    f = requests.get(link)
    return (f.json())

def transform(**context):
    extract_json = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    daily_list = extract_json["daily"][1:] # 오늘 ~ 7일 후 정보이므로, 내일 정보부터 7개 가져오기
    return daily_list

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    create_table(cur, schema, table)
    daily_list = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    try:
        sql = "DELETE FROM {schema}.{table};".format(schema=schema, table=table)
        for day in daily_list:
            dt = datetime.fromtimestamp(day["dt"]).strftime("%Y-%m-%d")
            day_temp = day["temp"]["day"]
            min_temp = day["temp"]["min"]
            max_temp = day["temp"]["max"]
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{dt}', {day_temp}, {min_temp}, {max_temp});"""
        logging.info(sql)
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    return

dag_weather_forecast_assignment = DAG(
    dag_id = 'weather_forecast_assginment',
    start_date = datetime(2023, 2, 4),
    schedule_interval = '0 9 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url': Variable.get("openweather_url")
    },
    dag = dag_weather_forecast_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = {
    },
    dag = dag_weather_forecast_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'foxbonbon',
        'table': 'weather_forecast'
    },
    dag = dag_weather_forecast_assignment)

extract >> transform >> load
