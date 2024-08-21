from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("hdfs") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df_medio = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_medio.csv", header=False).withColumnRenamed("_c0", "id")
df_medio.show()
df_grande = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_grande.csv", header=False).withColumnRenamed("_c0", "id")
df_grande.show()

#time.sleep(30000)
spark.stop()
