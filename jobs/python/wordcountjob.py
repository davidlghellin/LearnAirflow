from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"

words = spark.sparkContext.parallelize(text.split(" "))

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])
time.sleep(30000)

spark.stop()