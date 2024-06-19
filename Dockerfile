FROM apache/airflow:2.9.2

USER root
RUN apt-get update && \
    apt-get install -y zip unzip openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64


USER airflow

#RUN pip install --upgrade pip; pip install apache-airflow apache-airflow-providers-apache-spark pyspark
