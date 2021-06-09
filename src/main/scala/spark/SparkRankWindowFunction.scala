package spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object SparkRankWindowFunction extends InitSpark {

  def main(args: Array[String]) = {

    import spark.implicits._
    val sc = spark.sparkContext

    var df=Seq(1,2,3,4,5).toDF("num")

    df =df.withColumn("mod", $"num"%3)

    val windodSpec = Window.partitionBy("mod").orderBy("num")

    df.withColumn("windowed",dense_rank() over windodSpec).show
    close
  }


}
