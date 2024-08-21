from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("PythonWordCount").master("spark://spark-master:7077").getOrCreate()

text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"

words = spark.sparkContext.parallelize(text.split(" "))

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

dfword = spark.createDataFrame(wordCounts,["key", "value"])
dfword.show()

rdd = spark.sparkContext.parallelize(
  [
    ("first", (2.0, 1.0, 2.1, 5.4)),
    ("test", (1.5, 0.5, 0.9, 3.7)),
    ("choose", (8.0, 2.9, 9.1, 2.5))
  ]
)

dfWithoutSchema = spark.createDataFrame(rdd,["otro", "array_values"])

dfWithoutSchema.show()


#time.sleep(30000)
spark.stop()
