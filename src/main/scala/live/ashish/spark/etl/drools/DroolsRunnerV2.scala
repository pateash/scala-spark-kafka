package live.ashish.spark.etl.drools

import live.ashish.spark.etl.drools.DroolsRunnerUtils.processPartitionPerSession
import org.apache.spark.sql.{Row, SparkSession}
import org.kie.api.KieBase
import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}
import org.kie.internal.io.ResourceFactory
import org.kie.internal.utils.KieHelper

// use jdk8
// Using KieHelper
object DroolsRunnerV2 {
//  val ruleFilePath = "data/drools-input/person_rules.drl"
  val ruleFilePath = "rules/person_rules2.drl"
//  val ruleFilePath = "src/main/resources/rules/person_rules2.drl"
//  val ruleFilePath = "rules/person_rules2.drl"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroolsRuleEngineV2")
      .master("local[*]")
      .getOrCreate()

    val kieHelper = new KieHelper()
    val drlResource = ResourceFactory.newClassPathResource(ruleFilePath)
    kieHelper.addResource(drlResource)

    val results = kieHelper.verify()

    if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
      println("Errors occurred during rule verification:")
      results.getMessages(org.kie.api.builder.Message.Level.ERROR).forEach(message => {
        println("Error: " + message.getText)
      })
    }
    // Build a KieBase using the KieHelper
   implicit val kieBase: KieBase = kieHelper.build()
    println("Number of loaded rules: " + kieBase.getKiePackages.iterator().next.getRules.size())

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
