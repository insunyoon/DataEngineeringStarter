# incremental update + 슬랙 넣어보기

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
from plugins import slack

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
    temp_table = f"""temp_{table}"""
    
    cur = get_Redshift_connection()
    create_table(cur, schema, table) # weather_forecast가 없는 경우 실행
    daily_list = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    # incremental process
    temp_sql = f"""DROP TABLE IF EXISTS {schema}.{temp_table};
    CREATE TABLE {schema}.{temp_table} AS SELECT * FROM {schema}.{table};""" # temp 테이블에 복사
    # 참고: CTAS 단계에서 default column value값이 저절로 옮겨지지 않아 None으로 표시됨 -> 이후 원래 테이블로 복사할 때 고려 필요
    
    cur.execute(temp_sql)
    
    # 슬랙으로 보낼 text
    slack_txt = "1주일 일기예보 :wink:"
    
    ## temp 테이블에 새로운 정보 추가 (중복 생김)
    for day in daily_list:
        dt = datetime.fromtimestamp(day["dt"]).strftime("%Y-%m-%d")
        day_temp = day["temp"]["day"]
        min_temp = day["temp"]["min"]
        max_temp = day["temp"]["max"]
        temp_sql += f"""INSERT INTO {schema}.{temp_table} VALUES ('{dt}', {day_temp}, {min_temp}, {max_temp});"""
        
        # 슬랙으로 보낼 text도 추가
        slack_txt += f"""
        {dt}의 :cold_face: 최저 온도 - {min_temp} :hot_fire: 최고 온도 - {max_temp}"""
        
    logging.info(temp_sql)
    cur.execute(temp_sql)
    cur.execute("COMMIT;")



    ## TRANSACTION 처리
    ### 테이블 데이터 삭제 + 중복 제거한 데이터 넣기
    #### 위에서 None -> getdate 재처리, None이 더 최신 데이터로 잡힘
    try:
        sql = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT date, temp, min_temp, max_temp, ISNULL(created_date, GETDATE())
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
            FROM {schema}.{temp_table}
        )
        WHERE seq = 1;"""
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    return slack_txt


dag_weather_forecast_assignment = DAG(
    dag_id = 'weather_forecast_assginment_v2',
    start_date = datetime(2023, 2, 4),
    schedule_interval = '10 * * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_success_callback': slack.on_success_callback,
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
