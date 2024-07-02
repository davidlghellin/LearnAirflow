FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && \
    apt-get install -y zip unzip openjdk-17-jdk && \
    apt-get clean

USER airflow

#RUN pip install --upgrade pip; pip install apache-airflow apache-airflow-providers-apache-spark pyspark
