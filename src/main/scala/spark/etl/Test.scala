package spark.etl


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
