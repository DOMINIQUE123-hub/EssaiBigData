
import org.apache.spark.rdd._
//import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
//import  org.apache.hive._

import java.io.FileNotFoundException
//import java.util.logging._
//import scala.reflect.io.File
//import scala.sys.env


object DataFrame {

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
      .csv(chemin)

    df_test.show(15)

    val df_gp = session_s.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/Users/DELL E7470/Documents/Maîtrisez Spark pour le Big Data avec Scala/Resources/SOURCES DE DONNEES/csvs/")

    df_gp.show(7)
    println("df_test count : " + df_test.count() + "df_group count : " + df_gp.count())


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
