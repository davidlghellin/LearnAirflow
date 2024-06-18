from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag_args={
    "depends_on_past": False,
    #"email": ["david@david.es"]
    "email_on_failure":False,
    "email_on_retry":  False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

dag = DAG(
    "test1",
    description="Mi primer DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["ejemplo"]
)

def tarea0_fun(**kwargs):
    return { "ok": 0 }

def tarea2_fun(**kwargs):
    return { "ok": 2 }

tarea0 = PythonOperator(
    task_id="tarea0",
    python_callable=tarea0_fun,
    dag=dag
)

tarea1 = BashOperator(
    task_id="print_date",
    bash_command='echo "la fecha es $(date)"',
    dag=dag
)

tarea2 = PythonOperator(
    task_id="tarea2",
    python_callable=tarea2_fun,
    dag=dag
)

tarea0 >> [ tarea1, tarea2 ]