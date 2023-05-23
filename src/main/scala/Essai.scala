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


object Essai {

  var ss: SparkSession = null
  var spConf: SparkConf = null

  private var trace_log: Logger = LogManager.getLogger("Logger_Console")

  def main(args: Array[String]): Unit = {
    val sc = Session_Spark(Env = true).sparkContext
    val session_s = Session_Spark(true)

    sc.setLogLevel("OFF")

    val rdd_test: RDD[String] = sc.parallelize(List("lundi", "mardi", "mercredi", "jeudi", "vendredi"))

    //rdd_test.foreach{ l => println(l)}
    println()

    val rdd2 : RDD[String] = sc.parallelize(Array("Ajoua", "Mariam", "Konan", "Coulibaly"))
    //rdd2.foreach{l => println(l)}
    println()

    val rdd3 = sc.parallelize(Seq(("yao", "math", 12), ("kone", "math", 15), "Tia", "math", 20))


    //rdd3.take(1).foreach{ l => println(l) }

    //if (rdd3.isEmpty()){
    //  println("Le RDD est vide")
   // } else {
     //rdd3.foreach{ l => println(l)}
    //}

    //rdd3.saveAsTextFile("C://Users//DELL E7470//Documents//Maîtrisez Spark pour le Big Data avec Scala//ENREGISTREMENT_MANIPULATION//here1")

    val rdd4 = sc.textFile("C://Users//DELL E7470//Documents//Maîtrisez Spark pour le Big Data avec Scala//ENREGISTREMENT_MANIPULATION//lesrdd.txt")
    //rdd4.foreach{l => println(l)}

    val rdd_trans: RDD[String] = sc.parallelize(List("alain mange une banane", "la banane est un bon aliment pour la santé", "acheter une bonne banane"))

    val rdd_map = rdd_trans.map(x => x.split(" "))


    //println("le nombre d'éléments de mon rdd est : " +rdd_map.count())

    val rdd6 = rdd_trans.map(w => (w, w.length, w.contains("une"))).map(x => (x._1.toLowerCase(), x._2, x._3))

    rdd6.foreach(l => println(l))
    //val rdd6 = rdd_trans.map(a => (a, a.length, a.contains("banane"))).map(x => (x._1.toLowerCase, x._2, x._3))

    val rdd7 = rdd_trans.flatMap(x => x.split(" ")).map(w => (w, 1))
    rdd7.foreach(l => println(l))

    val rdd_comptage = rdd4.flatMap(x => x.split(" ")).map(w => (w, 1))
    rdd_comptage.foreach(l => println(l))

    val rdd_reduce = rdd_comptage.reduceByKey((x,y) => x+y)
    rdd_reduce.foreach(l => println(l))

    import session_s.implicits._  // Pour pouvoir transformer les RDD en dataframe

    val df : DataFrame = rdd7.toDF("teste", "valeur")
    df.show()

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
