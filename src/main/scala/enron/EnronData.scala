package enron

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object EnronData {

  def readEnronEmail(spark: SparkSession): RDD[EnronEmail] = {
    // Use a combination of `sc.textFile`, `filePath` and `parse`
    val rdd = readEnronEmailDf(spark).rdd
    // val rdd = readEnronEmailDfFromJdbc(spark).rdd
    rdd.map(
      row => EnronEmail(
        row.get(0) match {
          case x: Integer => x // required for JDBC
          case x: String => x.toInt
        },
        row.getString(1),
        row.get(2) match {
          case s: java.sql.Timestamp => s.toString  // required for JDBC
          case s: String => s.toLowerCase
        },
        row.getString(3),
        row.getString(4) match {
          case null => ""
          case s => s.toLowerCase
        },
        row.getString(5).toLowerCase,
        row.getString(6).toLowerCase
      )
    )
  }

  private def readEnronEmailDf(spark: SparkSession) = {
    spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load(resourcePath("messages.csv"))
  }

  private def readEnronEmailDfFromJdbc(spark: SparkSession) = {
    spark.sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1/enron")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "message")
      .option("user", "root")
      .option("password", "example").load()
  }

  def resourcePath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString
}
