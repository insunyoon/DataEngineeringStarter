from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """
    sql += select_sql
    cur.execute(sql)

    cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")

    try:
        sql = f"""DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};"""
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id = "Nps_Summary",
    start_date = datetime(2023,2,18),
    schedule_interval = '0 9 * * *',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'foxbonbon',
        'table': 'nps_summary',
        'sql' : """SELECT TO_CHAR(created_at, 'YYYY-mm-dd') nps_date, 
        ROUND((COUNT(CASE WHEN (score >= 9) THEN 1 ELSE NULL END) - COUNT(CASE WHEN (score <= 6) THEN 1 ELSE NULL END))::float*100 / COUNT(*)::float, 2) nps_pct 
        FROM foxbonbon.nps GROUP BY to_char(created_at, 'YYYY-mm-dd')
        ORDER BY nps_date;"""
    },
    provide_context = True,
    dag = dag
)
