package es.david

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.rand

object GroupBy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SaltingGroupBy")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
//    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
//
//    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1B")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/xyz_medio.csv")

    val iniTime = System.currentTimeMillis()
    df.groupBy("y")
      .count()
      .show()
    val finTime = System.currentTimeMillis()
    println(s"Tiempo de ejecución: ${finTime - iniTime} ms")

    val iniTime2 = System.currentTimeMillis()
    df.withColumn("salt", rand() * 5 cast ("int"))
      .groupBy("y", "salt")
      .count()
      .groupBy("y")
      .sum("count")
      .show()

    val finTime2 = System.currentTimeMillis()

    println(s"Tiempo de ejecución: ${finTime2 - iniTime2} ms")
    Thread.sleep(1000000)
  }
}
