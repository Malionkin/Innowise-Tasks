import os
from airflow.utils.timezone import datetime
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from queries import sql_queries as sq
from tasks import tasks_for_dag as td

load_dotenv()
connection_id = os.getenv("sf_con_id")


with DAG("SNOWFLAKE_TASK", schedule_interval=None, catchup=False, start_date=datetime(2022, 7, 18, 0, 0)) as dag:
    csv_into_table = PythonOperator(task_id="parse_csv_and_load_to_table", python_callable=td.parse_file)

    create_tables = SnowflakeOperator(
        task_id='create_tables_and_streams',
        snowflake_conn_id=connection_id,
        sql=sq.CREATE_TABLES_AND_STREAMS
    )

    insert_to_stage_table = SnowflakeOperator(
        task_id='insert_data_into_stage_table',
        snowflake_conn_id=connection_id,
        sql=sq.INSERT_STAGE_TABLE
    )
    insert_to_master_table = SnowflakeOperator(
        task_id='insert_data_into_master_table',
        snowflake_conn_id=connection_id,
        sql=sq.INSERT_MASTER_TABLE
    )

    create_tables >> csv_into_table >> insert_to_stage_table >> insert_to_master_table