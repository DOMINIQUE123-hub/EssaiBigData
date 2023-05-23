
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.rocksdb.Env
//import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._  // pour les jointures
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.{FileSystem, Path}

//import  org.apache.hive._

import java.io.FileNotFoundException
//import java.util.logging._
//import scala.reflect.io.File
//import scala.sys.env


object test {

  var ss: SparkSession = null
  var spConf: SparkConf = null

  private var trace_log: Logger = LogManager.getLogger("Logger_Console")

  /* CREATION DE STRUCTURE DE DATAFRAME */

  val schema_order = StructType(Array(
    StructField("orderid", IntegerType, false),
    StructField("customerid", IntegerType, false),
    StructField("campaignid", IntegerType, true),
    StructField("orderdate", TimestampType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("zipcode", StringType, true),
    StructField("paymenttype", StringType, true),
    StructField("totalprice", DoubleType, true),
    StructField("numorderlines", IntegerType, true),
    StructField("numunits", IntegerType, true)

  ))



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

    //df_test.show(6)
    val df = df_test.select(
      col("_c0").alias("Identifiant"),
      col("InvoiceNo"),
      col("Description"),
      col("StockCode").cast(IntegerType),
      col("Invoice".concat("No")).alias("ID_commande")
    )
    //df.show(4)
    //df.printSchema()
    //df.write.mode(SaveMode.Overwrite).csv("C:\\Users\\DELL E7470\\Documents\\Maîtrisez Spark pour le Big Data avec Scala\\ENREGISTREMENT_MANIPULATION\\here1")

    val df3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
      .withColumn("StockCode", col("StockCode").cast(IntegerType))
      .withColumn("val_const", lit(56))
      .withColumnRenamed("_c0", "ID_client")
      .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("ID_client")))
      .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
      .withColumn("Created_dt", current_timestamp())


    //df_test.show(5)
    //df3.show(5)

    /*Aggrégation et jointure*/

    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .load("C:\\Users\\DELL E7470\\Documents\\Maîtrisez Spark pour le Big Data avec Scala\\Resources\\orders.txt")

    /* Manipulation des dates dans Spark */




    val df_ordersgood = df_orders.withColumnRenamed("numunits", "numunits_orders")
      .withColumnRenamed("totalprice", "totalprice_order")

    df_ordersgood.withColumn("date_lecture", date_format(current_date(), "dd/MM/yyyy"))
      .withColumn("date_lecture_complete", current_timestamp())
      .withColumn("periode_seconde", window(col("date_lecture_complete"), "5 seconds"))
      .show(10)

    val df_product = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("C:\\Users\\DELL E7470\\Documents\\Maîtrisez Spark pour le Big Data avec Scala\\Resources\\product.txt")

    val df_orderline = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("C:\\Users\\DELL E7470\\Documents\\Maîtrisez Spark pour le Big Data avec Scala\\Resources\\orderline.txt")

    val df_joinorders = df_orderline.join(df_ordersgood, df_ordersgood.col("orderid") === df_orderline.col("orderid"), "inner")
      .join(df_product, df_product.col("productid") === df_orderline.col("productid"), Inner.sql)
    //df_joinorders.printSchema()


    /* Aggrégats et fenêtrage */



    df_joinorders.withColumn("total_amount", round(col("numunits") * col("totalprice"), 3))
      .groupBy("city")
      .sum("total_amount").as("commande").write
      .mode(SaveMode.Overwrite)
      //.save("C:\\Users\\DELL E7470\\Documents\\Maîtrisez Spark pour le Big Data avec Scala\\ENREGISTREMENT_MANIPULATION\\Ecriture")
      //.show()


    /* Travailler avec hdfs */

  }

  /*def spark_hdfs () : Unit = {
    val config_fs = Session_Spark (Env = true).sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config_fs)

    val src_path = new Path("/user/datalake/marketing/")
    val dest_path = new Path("/user/datalake/indexes/")

    // Lecture des fichiers d'un dossier
    // Première possibilité
    val liste_file = fs.listStatus(src_path)
    liste_file.foreach(f => println(f.getPath))

    // Deuxième possibilité
    val liste_file1 = fs.listStatus(src_path)
    for (i <- 1 to liste_file1.length){
      println(liste_file1(i))
    }

  }
  */



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
