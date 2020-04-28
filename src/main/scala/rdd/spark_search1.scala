package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object spark_search1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Spark-Assignment")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val readme = sc.textFile("C:\\Spark\\README.md")

    val word = readme.flatMap(kelime => kelime.split(" "))

    val a = "spark"
    val b = "Spark"

    val spark1 = word.filter(_.contains("spark"))

    val Spark2 = word.filter(_.contains("Spark"))

    val c = spark1.count() + Spark2.count()

    println(c)
    // 34

  }
}
