import test._
import java.util._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spark_DB {

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(Env = true)

    // Lecture d'une base de données Mysql

    val props_mysql = new Properties()
    props_mysql.put("user", "consultant")
    props_mysql.put("password", "pwd#86")

    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC", "jea_db.orders", props_mysql)


    //df_mysql.show(10)

    val df_mysql2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city from jea_db.orders) table_summary")
      .load()

    //df_mysql2.show(10)

    // Lecture d'une base de données PostgreSQL

    val prop_postgreSQL = new Properties()
    prop_postgreSQL.put("user", "postgres")
    prop_postgreSQL.put("password", "Postgre@123")

    val df_postrgre = ss.read.jdbc("jdbc:postgresql://127.0.0.1:5432/essai", "etudiant", prop_postgreSQL)

    df_postrgre.show()



  }

}
