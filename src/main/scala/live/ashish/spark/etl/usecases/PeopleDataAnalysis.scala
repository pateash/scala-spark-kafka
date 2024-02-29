package live.ashish.spark.etl.usecases

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import TestSkipHeader.reader



object PeopleDataAnalysis{
  def main(args: Array[String]) = {
    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder()
      .appName("TEST APP  ")
      .master("local[*]")
      .config("option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val version = spark.version
    println("SPARK VERSION = " + version)

//    val sumHundred = live.ashish.spark.range(1, 101).reduce(_ + _)
//    println(f"Sum 1 to 100 = $sumHundred")

    println("Reading from csv file: people-example.csv")
    val persons = reader.csv("data/csv/people-example.csv").as[Person]
    persons.show(2)
    val averageAge = persons.agg(avg("age"))
                     .first.get(0).asInstanceOf[Double]
    println(f"Average Age: $averageAge%.2f")

  }
  final case class Person(firstName: String, lastName: String,
                          country: String, age: Int)

}
