package live.ashish.spark.etl.drools

import live.ashish.spark.etl.drools.DroolsRunnerUtils.processPartitionPerSession
import org.apache.spark.sql.{Row, SparkSession}
import org.kie.api.builder.Message
import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}
import org.kie.api.{KieBase, KieServices}

// use jdk8
// this creates single session for each partition and per row
object DroolsRunner {
//  val ruleFilePath = "data/drools-input/person_rules.drl"
//  val ruleFilePath = "src/main/resources/rules/person_rules2.drl"
  val ruleFilePath = "rules/person_rules2.drl"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroolsRuleEngine")
      .master("local[*]")
      .getOrCreate()

    val kieServices = KieServices.Factory.get()
    //    val drlContent = scala.io.Source.fromFile(ruleFilePath).mkString
    //    println(drlContent)
    //    val kieFileSystem = kieServices.newKieFileSystem()
//      .write(ruleFilePath, kieServices.getResources.newByteArrayResource(drlContent.getBytes))
    val kieFileSystem = kieServices.newKieFileSystem()
      .write(ruleFilePath, kieServices.getResources.newClassPathResource(ruleFilePath));
//      .write(ruleFilePath, kieServices.getResources.newClassPathResource("live.ashish.spark.etl.droolsperson_rules2.drl"));

    val kieBuilder = kieServices.newKieBuilder(kieFileSystem)
    val results = kieBuilder.buildAll()
    println(results)
    if (results.getResults.hasMessages(Message.Level.ERROR)) {
      val errors = results.getResults.getMessages(Message.Level.ERROR)
      errors.forEach(error => {
        println(s"Error: ${error.getText}")
      })
    }
    val kieContainer = kieServices.newKieContainer(kieServices.getRepository.getDefaultReleaseId)
    implicit val kieBase: KieBase = kieContainer.getKieBase()

    val data = Seq(
      ("Stewie", 3, "UNKNOWN"),
      ("Alan", 15, "UNKNOWN"),
      ("Alice", 25, "UNKNOWN"),
      ("Bob", 60,  "UNKNOWN"),
      ("Charlie", 18,  "UNKNOWN")
    )
    val columns = Seq("name", "age", "classification")
    val df = spark.createDataFrame(data).toDF(columns: _*)
    df.show()

    val resultRDD2 = df.rdd.mapPartitions(processPartitionPerSession)
    val resultDF2 = spark.createDataFrame(resultRDD2, df.schema)
    resultDF2.show()
    spark.stop()
    }
}
