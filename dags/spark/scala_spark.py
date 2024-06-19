import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "spark_scala",
    default_args = {
        "owner": "David",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily",
    tags=["spark", "scala"]
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

scala_job = SparkSubmitOperator(
    task_id="scala_job",
    conn_id="spark-david",
    application="jobs/scala/wordcount/target/scala-2.12/word-count_2.12-0.1.jar",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> [scala_job] >> end
