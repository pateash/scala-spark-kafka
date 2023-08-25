//package spark.etl.usecases
//
//import org.apache.spark.sql.{Row, SparkSession}
//import org.kie.api.KieServices
//import org.kie.internal.io.ResourceFactory
//
//object DroolsRuleEngine2 {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("DroolsRuleEngine")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//    val ruleFilePath = "data/drools-input/traffic.drl"
//    val drlContent = scala.io.Source.fromFile(ruleFilePath).mkString
//
////    val hdfsRuleFilePath = "hdfs://your-hdfs-host:port/path/to/your/rules.drl"
////    val drlContent = spark.read.textFile(hdfsRuleFilePath).collect().mkString("\n")
//
//    val kieServices = KieServices.Factory.get()
//    val kieFileSystem = kieServices.newKieFileSystem()
//      .write(ResourceFactory.newByteArrayResource(drlContent.getBytes))
//
//    val kieBuilder = kieServices.newKieBuilder(kieFileSystem).buildAll()
//    val kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId())
//
//    val data = Seq(
//      ("Alice", 25),
//      ("Bob", 30),
//      ("Charlie", 18)
//    )
//    val columns = Seq("name", "age")
//    val df = spark.createDataFrame(data).toDF(columns: _*).as[Traffic]
//
////    val df = spark.read.csv("data/drools-input/Sample.csv")
////    val columns = df.columns
//    // Define a function to process each partition
//    def processPartition(iterator: Iterator[Traffic]): Iterator[(String, Int)] = {
//      val kieSession = kieContainer.newKieSession()
//      try {
//        iterator.map { case Traffic(cid,traffic_light, driving_style) as record =>
////          val person = t
//          kieSession.insert(record)
//          kieSession.fireAllRules()
//          record // Return the modified person
//        }
//      } finally {
//        kieSession.dispose()
//      }
//    }
//
//    def processPartition2(iterator: Iterator[Row]): Iterator[Row] = {
//      val kieSession = kieContainer.newKieSession()
//      try {
//        iterator.map { case Traffic(cid, traffic_light, driving_style) as record =>
//          //          val person = t
//          kieSession.insert(record)
//          kieSession.fireAllRules()
//          record // Return the modified person
//        }
//      } finally {
//        kieSession.dispose()
//      }
//    }
//
//    // Apply the processPartition function to each partition
//    val resultRDD = df.mapPartitions(processPartition)
//
//    // Convert the RDD back to a DataFrame
//    val resultDF = spark.createDataFrame(resultRDD).toDF(columns: _*)
//
//    resultDF.show()
//
//    spark.stop()
//  }
//
//  // A simple class representing a person
////  case class Person(name: String, age: Int)
//  case class Traffic(cid: Int, traffic_light: String,driving_style: String)
//}
