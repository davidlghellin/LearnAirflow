package es.david

import es.david.KryoSerializador.usuariosDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object JavaSerializador {
  // spark-submit --class es.david.JavaSerializador basic-job-scala_2.12-0.1.jar

  val spark = SparkSession
    .builder
    .appName("Java Serializador")
    // .master("spark://spark-master:7077")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  def generarUser(nUser: Int) = (1 to nUser).map(n => Usuario(s"dni$n", n, s"n@$n.com"))


  val usuarios = sc.parallelize(generarUser(1000000))
  val usuariosDF: DataFrame = sc.parallelize(generarUser(1000000)).toDF()

  def main(args: Array[String]): Unit = {
    usuarios.persist(StorageLevel.MEMORY_ONLY_SER).count()
    usuariosDF.show()
    usuariosDF.repartition()

    usuariosDF.repartition(5).write.parquet("/Users/davidlopez/particionado")
    println("******* Esperamos *******")
    Thread.sleep(111111111)
  }
}

case class Usuario(dni: String, anyo: Float, email: String)

