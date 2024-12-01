package es.david

import org.apache.spark.sql.{DataFrame, SparkSession}


object CambioNameJobSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Cambio Nombre de Job en Spark UI")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    spark.sql("""show databases""").show
    spark.sql("""DROP TABLE IF EXISTS default.table_name""")
    spark.sql("""Create TABLE IF NOT EXISTS default.table_name (a string, b int)""")
    spark.sql("""Insert into default.table_name values ('hola',2)""")

    def runSQLSFromString(query: String, sqlCount: Int): Unit = {
      spark.sparkContext.setLocalProperty("callSite.short", "SQL_" + sqlCount.toString)
      spark.sparkContext.setJobDescription(query)
      spark.sql(query)
    }

    runSQLSFromString("""Insert into default.table_name values ('hola',3)""", 1)
    runSQLSFromString("""Insert into default.table_name values ('hola',4)""", 2)

    //vamos a hacer un conteo
    spark.sparkContext.setLocalProperty("callSite.short", "count_persistir_por_ejemplo")
    spark.sparkContext.setJobDescription("hacemos el count")
    spark.sql("SELECT * FROM default.table_name").count()

    // hacemos union
    val df = spark.sql("SELECT * FROM default.table_name").union(spark.sql("SELECT * FROM default.table_name"))
    spark.sparkContext.setLocalProperty("callSite.short", "count_tocho")
    spark.sparkContext.setJobDescription("hacemos el count despues del pedazo union")
    spark.sql("SELECT * FROM default.table_name").count()
    // otra
    //    spark.sparkContext.setLocalProperty("callSite.long","AAAAAAA")
    spark.sparkContext.setLocalProperty("callSite.short", "insert")
    spark.sparkContext.setJobDescription("otr insert")
    spark.sql("""Insert into default.table_name values ('hola',3)""")

    spark.sparkContext.setLocalProperty("callSite.short", null)
    spark.sparkContext.setJobDescription(null)
    spark.sql("SELECT * FROM default.table_name").count()

    Thread.sleep(Long.MaxValue)
  }
}

