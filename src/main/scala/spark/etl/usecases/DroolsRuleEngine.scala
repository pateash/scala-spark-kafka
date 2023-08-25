package spark.etl.usecases

import org.apache.spark.sql.{Row, SparkSession}
import org.drools.compiler.kie.builder.impl.KieServicesImpl
import org.kie.api.KieServices
import org.kie.api.io.ResourceType
import org.kie.internal.io.ResourceFactory

object DroolsRuleEngine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroolsRuleEngine")
      .master("local[*]")
      .getOrCreate()
    val ruleFilePath = "data/drools-input/person_rules.drl"
    val drlContent = scala.io.Source.fromFile(ruleFilePath).mkString

//    val hdfsRuleFilePath = "hdfs://your-hdfs-host:port/path/to/your/rules.drl"
//    val drlContent = spark.read.textFile(hdfsRuleFilePath).collect().mkString("\n")

//    val kieServices = KieServices.Factory.get()
    val kieServices = new KieServicesImpl()
    val kieFileSystem = kieServices.newKieFileSystem()
//      .write(ResourceFactory.newByteArrayResource(drlContent.getBytes))
//    val kieFileSystem = kieServices.newKieFileSystem.write("src/main/resources/rules/myRule.drl",
//    kieServices.getResources.newClassPathResource("myRule.drl"),   ResourceType.DRL)
    kieFileSystem.write(ruleFilePath,
      kieServices.getResources.newClassPathResource(ruleFilePath))
//      ResourceType.DRL)

    val kieBuilder = kieServices.newKieBuilder(kieFileSystem).buildAll()
    val kieContainer = kieServices.newKieContainer(kieServices.getRepository.getDefaultReleaseId)

    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 18)
    )
    val columns = Seq("name", "age")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    def processPartitionPeople(iterator: Iterator[Row]): Iterator[Row] = {
      val kieSession = kieContainer.newKieSession()
      try {
        iterator.map { row =>
          val person = Person(row.getAs[String]("name"),row.getAs[Int]("age"))
          kieSession.insert(person)
          kieSession.fireAllRules()
          Row.fromTuple(person.name, person.age) // Return the modified person
        }
      } finally {
        kieSession.dispose()
      }
    }

    // Apply the processPartition function to each partition
    val resultRDD = df.rdd.mapPartitions(processPartitionPeople)

    println(resultRDD)
    // Convert the RDD back to a DataFrame
//    val resultDF = spark.createDataFrame(resultRDD).toDF(columns: _*)
//
//    resultDF.show()

    spark.stop()
  }

  // A simple class representing a person
  case class Person(name: String, age: Int)
}
