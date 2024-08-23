from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("PythonWordCount").master("spark://spark-master:7077").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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

# DataFrames de ejemplo
df1 = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "value1"])
df2 = spark.createDataFrame([(1, "X", "Y"), (2, "Z", "X"), (4, "Y", "Z")], ["id1", "id2", "value2"])
df1.show()
df2.show()

# Primer join con la primera condición
join1 = df1.join(df2, df1["id"] == df2["id1"], "inner")
join1.explain()
# Segundo join con la segunda condición
join2 = df1.join(df2, df1["id"] == df2["id2"], "inner")
join1.show()
join2.show()

# Unir ambos resultados
resutl = join1.union(join2)
resutl.explain()
resutl.show()

time.sleep(30000)
spark.stop()
