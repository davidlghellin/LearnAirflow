from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("hdfs") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_medio = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_medio.csv", header=False).withColumnRenamed("_c0", "id")
df_medio.show()
df_grande = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_grande.csv", header=False).withColumnRenamed("_c0", "id")

num_particiones = spark.conf.get("spark.sql.shuffle.partitions")
df_salt = df_grande \
        .withColumn("salt", F.lit(F.floor(F.rand() * num_particiones)))
#df_salt.show()
#print(f"el count es {df_salt.count()}")
#df_salt.select("salt").groupBy("salt").agg(count("*")).show()

#df_salt.groupBy(["salt","id"]).agg(count("*")).orderBy(["id","salt"]).show()

df_2 = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_medio.csv", header=False).withColumnRenamed("_c0", "id2")
# dev
##df_2    = df_2.where(df_2.id2 == "y")
##df_medio = df_medio.where(df_medio.id == "y")

#df_medio.show()
df_grande = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_grande.csv", header=False).withColumnRenamed("_c0", "id")
#df_grande.show()

df_medio.select("id").groupBy("id").agg(count("*")).show()
"""
+---+--------+
| id|count(1)|
+---+--------+
|  x|10000000|
|  z|       1|
|  y|       1|
+---+--------+
"""

inicio = time.time()
# Código a medir
#df_grande.select("id").groupBy("id").agg(count("*")).show()

"""
+---+----------+
| id|  count(1)|
+---+----------+
|  x|1000000000|
|  z|         4|
|  y|         4|
+---+----------+

df_result = df_grande.join(
  df_medio, df_grande.id == df_medio.id
).select(df_grande["id"])
df_result.groupBy("id").agg(count("*")).show()


"""


df_result = df_medio.join(
  df_2, df_medio.id == df_2.id2
).select(df_medio["id"])

df_result.groupBy("id").agg(count("*")).show()



df_medio = spark.read.csv("hdfs://learnairflow-namenode-1/xyz_medio.csv", header=False).withColumnRenamed("_c0", "id")
df_result = df_medio.withColumn(
    "id_modified",  # Nuevo nombre de la columna para evitar conflictos
    concat(
        df_medio["id"], 
        lit('_'), 
        floor(rand(123456) * 3).cast("string")  # Convierte el resultado a cadena para concatenar
    )
)
df_result.show()

df_explode = df_medio.withColumn(
    "explodeCol", 
    explode(array([lit(i) for i in range(4)]))  # range(4) genera los números 0 a 3
)

df_explode.show()

time.sleep(30000)
spark.stop()
