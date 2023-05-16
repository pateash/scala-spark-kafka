package spark.etl.usecases

import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
import _root_.io.prophecy.libs.FFSchemaRecord
import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.Json

object FixedFormatSkipHeaderFooter {

  def readFixedFormatDF(spark:SparkSession, path:Option[String], fixedSchemaString:Option[String], skipHeaders:Option[String] = None, skipFooters: Option[String] = None): DataFrame = {

    if (
      (skipHeaders.isDefined && skipHeaders.get != "0") ||
        (skipFooters.isDefined && skipFooters.get != "0")
    ) {
      import org.apache.spark.sql.Row
      val skipHeaderLines = skipHeaders.getOrElse("0").toInt

      val linesRDD = spark.sparkContext.textFile(path.get) // Read file as RDD of lines
      val totalCount = linesRDD.count()
      var skipFooterLines = 0
      if (totalCount > 0) {
        skipFooterLines = (totalCount - 1 - skipFooters.getOrElse("0").toInt).toInt
      }
      println("========= start ==========")
      linesRDD.collect().foreach(println)
      println("========= end ==========")
      linesRDD.zipWithIndex().collect.foreach(println)

      val fixedRDD = linesRDD.zipWithIndex // Add line number to each line
        .filter {
          case (data, idx) => (idx >= skipHeaderLines) & (idx <= skipFooterLines)
        } // Filter out first n lines

      println("========= filtered ==========")

      fixedRDD.collect.foreach(println)

      val finalRDD = fixedRDD.map { case (line, idx) => Row.fromSeq(line) }
      val schema = fixedSchemaString.map(s ⇒ parse(s).asInstanceOf[FFSchemaRecord]).get.toSpark
      spark.createDataFrame(finalRDD, schema)
    }
    else {
      var df: DataFrame = spark.emptyDataFrame
      try df = spark.read.option(
        "schema",
        Some(fixedSchemaString.get).map(s => parse(s).asInstanceOf[FFSchemaRecord])
          .map(s => Json.stringify(Json.toJson(s)))
          .getOrElse("")
      )
        .format("io.prophecy.libs.FixedFileFormat")
        .load(
          Some(path.get)
            .getOrElse("")
        )
      catch {
        case e: Error =>
          println(s"Error occurred while reading dataframe: $e")
          throw new Exception(e.getMessage)
        case e: Throwable =>
          println(s"Throwable occurred while reading dataframe: $e")
          throw new Exception(e.getMessage)
      }
      df
    }
  }
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder()
      .appName("TEST FixedFormat")
      .master("local[*]")
      .config("option", "some-value")
      .getOrCreate()

    val fixedSchemaString = Option("""
      |ascii record
      |string(5) custid;
      |string(13) address;
      |string(30) city;
      |string(16) county;
      |string(10) postal;
      |end
      |""".stripMargin)
    readFixedFormatDF(spark,Option("data/fixedformat/address_fixed_width.txt"),fixedSchemaString).show(false)

    readFixedFormatDF(
      spark,
      Option("data/fixedformat/address_fixed_width_header_footer.txt"),
      fixedSchemaString,
      skipHeaders = Option("3"),
      skipFooters = Option("3")
    ).show(false)

  }

}
