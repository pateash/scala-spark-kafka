package live.ashish.spark.etl.usecases

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object Main {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder()
      .appName("COVID ANALYSI")
      .master("local[*]")
      .config("option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("inferSchema", "true")
      .option("header","true")
      .csv("data/large-data/covid_data.csv") // not included this long data 60MB in code
      .selectExpr(
        "Country_Region as country",
        "Population as population",
        "Weight as weight",
        "Date as date",
        "Target as target",
        "TargetValue as targetValue"
      ).as[CovidData]

    df.cache()

    df.show(false)
//    df.describe().show(false)

    df
      .filter(_.country.equalsIgnoreCase("India"))
      .filter(_.target.equalsIgnoreCase("ConfirmedCases"))
      .show()

    df
      .filter(_.country.equalsIgnoreCase("India"))
      .write
      .option("header","true")
      .csv("data/csv/covid_date_india")


    // show

    //    df.groupBy("")
  }
  case class CovidData(country:String, population: Int, weight: Double, date: Timestamp, target: String, targetValue: Double)
}


