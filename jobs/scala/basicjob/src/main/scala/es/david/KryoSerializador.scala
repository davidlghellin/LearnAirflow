package es.david

import es.david.JavaSerializador.usuariosDF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object KryoSerializador {
  // spark-submit --class es.david.KryoSerializador basic-job-scala_2.12-0.1.jar

  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(Array(
      classOf[Usuario],
      classOf[Array[Usuario]]
    ))

  val spark = SparkSession
    .builder
    .appName("Kryo Serializador")
    .config(sparkConf)
   // .master("spark://spark-master:7077")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  def generarUser(nUser: Int) = (1 to nUser).map(n => Usuario(s"dni$n", n, s"n@$n.com"))

  val usuarios = sc.parallelize(generarUser(1000000))
  val usuariosDF: DataFrame = sc.parallelize(generarUser(1000000)).toDF()
  val userDataset: Dataset[Usuario] = usuariosDF.as[Usuario]

  def main(args: Array[String]): Unit = {
    usuarios.persist(StorageLevel.MEMORY_ONLY_SER).count()
    usuariosDF.show()
    import org.apache.spark.util.SizeEstimator
    SizeEstimator.estimate(usuariosDF)
    println("******* Esperamos *******")
    Thread.sleep(111111111)
  }
}

