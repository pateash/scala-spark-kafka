package live.ashish.spark.etl.drools

import live.ashish.spark.etl.drools.DroolsRunnerUtils.{Person, processPartitionPerSession}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.kie.api.KieBase
import org.kie.internal.io.ResourceFactory
import org.kie.internal.utils.KieHelper

// use jdk8 and reads a package for drools files
// Using KieHelper
object DroolsRunnerV3 {
  val ruleFilePath = "rules/classification/*"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroolsRuleEngineV3")
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

    val df = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/drools-input/People.csv")

    import spark.implicits._
//    implicit val e = Encoders.bean[Object](Class.forName("asdfadsf").asInstanceOf[Class[Object]])
//    df.as[Person].mapPartitions { persons =>
//      persons
//    }

    df.show()
    val resultRDD2 = df.rdd.mapPartitions(processPartitionPerSession)
    val resultDF2 = spark.createDataFrame(resultRDD2, df.schema)
    resultDF2.show()
    spark.stop()
    }
}
