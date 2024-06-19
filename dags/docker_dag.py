from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.docker.operators.docker import DockerOperator

default_args = {"owner": "David",}

@dag(default_args=default_args, start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False)
def docker_dag():

    @task()
    def t1():
        pass

    @task()
    def t2b():
        pass

    @task()
    def t3():
        pass

    t2a = DockerOperator(
        task_id='t2',
        image='python:3.8-slim-buster',
        container_name="Docker_desde_airflow",
        auto_remove=True,
        command='echo "run in docker image"',    # 'python3 my_scrypt.py
        docker_url='unix://var/run/docker.sock', # revisar permisos sudo chmod 555 /var/run/docker.sock
        network_mode='bridge',
        xcom_all=True   
    )

    t1() >> [ t2a, t2b() ] >> t3()

dag= docker_dag()


