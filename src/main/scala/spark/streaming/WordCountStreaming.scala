package spark.streaming

import spark.etl.InitSpark

/* Start port and provide output  ( refer  - https://spark.apache.org/docs/2.1.0/structured-streaming-programming-guide.html#quick-example )
*  $nc -lk 9999
*
* */
object WordCountStreaming extends InitSpark {
  def main(args: Array[String]) = {

    import spark.implicits._
    val version = spark.version
    println("SPARK VERSION = " + version)

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split("[^\\w]"))


    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete") // complete printout all output for all df, update only for required.
      .format("console")
      .start()

    query.awaitTermination()
  }
}
