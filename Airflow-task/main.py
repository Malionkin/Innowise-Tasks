import os
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pymongo import MongoClient

def prepare_environment(ti):
    load_dotenv()
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT"))
    input = os.getenv("INPUT")
    output = os.getenv("OUTPUT")
    ti.xcom_push("db_name", db_name)
    ti.xcom_push("db_host", db_host)
    ti.xcom_push("db_port", db_port)
    ti.xcom_push("input", input)
    ti.xcom_push("output", output)

def data_transform(ti):
    input = ti.xcom_pull("input", "prepare_environment")
    output = ti.xcom_pull("output", "prepare_environment")
    print(input)
    with open(input) as file:
        data = pd.read_csv(file)
    data.dropna(inplace=True)
    data.fillna("-", inplace=True)
    data.sort_values(by="at", inplace=True)
    data["content"].replace(r"[^\s\w,.!?'\\-]", "", regex=True, inplace=True)
    data.to_csv(output)

def load_to_db(ti):
    output = os.getenv("OUTPUT")
    db_name = ti.xcom_pull("db_name", "prepare_environment")
    db_host = ti.xcom_pull("db_host", "prepare_environment")
    db_port = ti.xcom_pull("db_port", "prepare_environment")
    with open(output) as file:
        data = pd.read_csv(file)
    client = MongoClient(db_host, db_port)
    db = client.db_name
    db.tiktok.drop()
    db.tiktok.insert_many(list(data.T.to_dict().values()))

with DAG("task_5_airflow", schedule_interval="@once", start_date=datetime(2021, 1, 1, 1, 1)) as dag:
    env_task = PythonOperator(task_id="prepare_environment", python_callable=prepare_environment)
    transform_task = PythonOperator(task_id="data_transform", python_callable=data_transform)
    load_task = PythonOperator(task_id="load_to_db", python_callable=load_to_db)
    env_task >> transform_task >> load_task
