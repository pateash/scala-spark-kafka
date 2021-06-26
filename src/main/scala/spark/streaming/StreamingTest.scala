package spark.streaming

import org.apache.spark.sql.SparkSession

object StreamingTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder()
      .appName("STREAMING_TEST")
      .master("local[*]")
      .config("option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val linesAgg = lines.as[String]
      .flatMap(_.split("[^\\w]"))
      .groupBy("value").count() // default column name of column

    val query = linesAgg.writeStream
      .outputMode("update")
      .format("console")
      .start()


    query.awaitTermination()



  }
}
