import test._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.when

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import scala.sys.env

object UseCaseBANO {

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)
  ))

  val configH = new Configuration()
  val fs = FileSystem.get(configH)

  def main(args: Array[String]): Unit = {
    val ss = Session_Spark(Env = true)

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", true)
      .schema(schema_bano)
      .csv("D:\\Maîtrisez Spark pour le Big Data avec Scala\\Resources\\SOURCES DE DONNEES\\full.csv")

    //df_bano_brut.show(10)

    val df_bano = df_bano_brut
      .withColumn("code_departement", substring(col("code_postal"), 1, 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreetMap"))
      .otherwise(when(col("code_source_bano") === lit("OD"), lit("OpenData"))
      .otherwise(when(col("code_source_bano") === lit("O+O"), lit("OpenData OSM"))
      .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
      .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre OSM")))))))

    //df_bano.show(10)

    val df_departement = df_bano.select(col("code_departement")).distinct()
      .filter(col("code_departement").isNotNull)

    val liste_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)
      .collect()
      .map(x => x(0)).toList

    //liste_departement.foreach(e => println(e.toString))

    //df_departement.show()


  }
  val chemin_src = new Path("D:\\Maîtrisez Spark pour le Big Data avec Scala\\Resources\\SOURCES DE DONNEES\\full.csv")
  val chemin_dest = new  Path("C:\\Users\\DELL E7470\\Documents\\EXAMEN BLANC 2023")

  fs.copyFromLocalFile(chemin_src, chemin_dest)

}
