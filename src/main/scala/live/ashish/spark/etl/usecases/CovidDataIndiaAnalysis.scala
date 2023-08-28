package live.ashish.spark.etl.usecases

import org.apache.spark.sql.SparkSession
import live.ashish.spark.etl.usecases.CovidDataAnalysis.CovidData

import java.sql.Timestamp

object CovidDataIndiaAnalysis {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder()
      .appName("COVID ANALYSIS INDIA")
      .master("local[*]")
      .config("option", "some-value")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read
      .option("inferSchema", "true")
      .option("header","true")
      .csv("data/csv/*_data_india*") // not included this long data 60MB in code
//    df.show(false)
    println("live.ashish")
    println(df.count())
  }
}


