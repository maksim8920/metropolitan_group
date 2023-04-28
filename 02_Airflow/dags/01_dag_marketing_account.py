from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import DAG
from datetime import datetime, timedelta
import pendulum


# Выставим таймзону Эмиратов:)
local_tz = pendulum.timezone("Asia/Dubai")


# Коннекшен к BiqQuery
bigquery_conn = 'my_bq_conn'


default_args = {
    'owner': 'maks',
    'dagrun_timeout': timedelta(hours=1)  # Установка времени выполнения DAG на 1 час
}


dag = DAG(
        'mart_data.marketing_account',
        start_date=pendulum.datetime(2023, 8, 24, tz=local_tz),
        schedule_interval='0 3 * * *',
        default_args=default_args
        )


task_marketing_account = BigQueryOperator(
    task_id='marketing_account',
    bigquery_conn_id=bigquery_conn,
    sql='sql/01_marketing_account.sql',
    dag=dag
)


task_marketing_account
