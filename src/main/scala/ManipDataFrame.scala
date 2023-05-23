
import org.apache.spark.rdd._
//import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
//import  org.apache.hive._

import java.io.FileNotFoundException
//import java.util.logging._
//import scala.reflect.io.File
//import scala.sys.env


object ManipDataFrame {

  var ss: SparkSession = null
  var spConf: SparkConf = null

  private var trace_log: Logger = LogManager.getLogger("Logger_Console")

  def main(args: Array[String]): Unit = {
    val sc = Session_Spark(Env = true).sparkContext
    val session_s = Session_Spark(true)

    sc.setLogLevel("OFF")


    val chemin = "C:\\Users\\DELL E7470\\Documents\\Maîtrisez Spark pour le Big Data avec Scala\\Resources\\SOURCES DE DONNEES\\2010-12-06.csv"
    val df_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true)
      .csv(chemin)

    //df_test.show(10)
    df_test.printSchema()
    val df2 = df_test.select(
      col("_c0").alias("Identifiant"),
      col("InvoiceNo"),
      col("Quantity"),
      col("StockCode").cast(IntegerType).alias("Code_marchandise")
    )

    df2.show(7)
    df2.printSchema()

  }

  def Session_Spark (Env : Boolean = true) : SparkSession = {
    try {


      if (Env == true) {
        System.setProperty("hadoop.home;dir", "C:/hadoop/bin")

        ss = SparkSession.builder
          .master("local")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      } else {

        ss = SparkSession.builder

          .appName("Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      }

    } catch {
      case ex : FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
      case ex : Exception => trace_log.error("Erreur dans l'initialisation de la Spark " + ex.printStackTrace())
    }
    return ss
  }

}
