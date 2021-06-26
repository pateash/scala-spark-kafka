package spark.etl.usecases

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.etl.InitSpark


object WordCount extends InitSpark {

  def main(args: Array[String]) = {

    val sc = spark.sparkContext

    val  rdds = sc.textFile("data/wordcount.txt")

    println(rdds.collect().mkString(","))

    WordCount1(spark,rdds)
    WordCount2(spark,rdds)

    close
  }

  def WordCount1(spark: SparkSession, rdds: RDD[String]) = {
    import spark.implicits._

    val output = rdds.flatMap(_.split("[^\\w]"))
      .map((_, 1))
      .reduceByKey(_ + _)

    val wordCountDF = output.toDF("word", "count")

    writeCsv(wordCountDF,"output/WCOutput1")
  }

  def WordCount2(spark: SparkSession, rdds: RDD[String]) = {
    import spark.implicits._

    val words=rdds
      .flatMap(x=>x.split("[^\\w]"))
      .map((_,1))
      .toDF("word","count")

    //    words.show(false)


    val wordCount=words.groupBy("word")
      .sum("count").as("count")
    //    wordCount.show(false)

    writeCsv(wordCount,"output/WCOutput2")

  }

  def writeCsv(df:DataFrame,path:String) ={
   df.coalesce(1)
     .write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv(path)
  }

}
