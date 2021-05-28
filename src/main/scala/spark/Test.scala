package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Test extends InitSpark {

  def main(args: Array[String]) = {

    val sc = spark.sparkContext
    val df=spark.read
      .option("inferSchema",true)
      .csv("data/wordcount.txt")

    df.show(false)
    print(df.count())

    close
  }


}
