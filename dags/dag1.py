# config
# https://www.mongodb.com/developer/products/mongodb/mongodb-apache-airflow/

import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta
import requests
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def getBTC():
    url = 'https://api.binance.com/api/v3/ticker?type=MINI&symbol=BTCUSDT&windowSize=1h'
    response = requests.get(url)
    data = response.json()
    print(data)
    return data

def uploadtomongo(ti, **context):
    try:
        hook = MongoHook(mongo_conn_id='mongoid')
        client = hook.get_conn()
        db = client.MyDB
        btc_collection=db.btc_collection
        print(f"Connected to MongoDB - {client.server_info()}")
        d = context["result"]
        btc_collection.insert_one(d)
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")
        

with DAG(
    dag_id="Get_BTC_and_Save_to_MongoDB",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 12, 21, tzinfo=local_tz),
    end_date=datetime(2024, 12, 24, 6, 0, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags= ["crypto"],
    default_args={
        "owner": "Chayaphon S.",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        'on_failure_callback': on_failure_callback
    }
) as dag:

    t1 = PythonOperator(
        task_id='get_btc',
        python_callable=getBTC,
        dag=dag
        )

    t2 = PythonOperator(
        task_id='upload_mongodb',
        python_callable=uploadtomongo,
        op_kwargs={"result": t1.output},
        dag=dag
        )

    t1 >> t2