package es.david

import org.apache.spark.sql.SparkSession

object WordCount extends App {
  val spark = SparkSession
    .builder
    .appName("BasicJob")
    .master("spark://spark-master:7077")
    .getOrCreate()

  val sc = spark.sparkContext

  val textData = spark
    .read
    .csv("hdfs://learnairflow-namenode-1/xyz_medio.csv")

  textData.show

  spark.stop()
}
