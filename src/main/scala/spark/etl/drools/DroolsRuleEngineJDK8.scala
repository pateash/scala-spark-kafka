package spark.etl.drools

import org.apache.spark.sql.{Row, SparkSession}
import org.kie.api.KieServices
import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}

// this creates single session for each row.
object DroolsRuleEngineJDK8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroolsRuleEngineJDK8")
      .master("local[*]")
      .getOrCreate()

    val ruleFilePath = "data/drools-input/person_rules.drl"
    val drlContent = scala.io.Source.fromFile(ruleFilePath).mkString

    println(drlContent)
    val kieServices = KieServices.Factory.get()
    val kieFileSystem = kieServices.newKieFileSystem()
      .write("data/drools-input/person_rules.drl", kieServices.getResources.newByteArrayResource(drlContent.getBytes))

    val kieBuilder = kieServices.newKieBuilder(kieFileSystem)
//    kieBuilder.
    kieBuilder.buildAll()
    val kieContainer = kieServices.newKieContainer(kieServices.getRepository.getDefaultReleaseId)
    val kieBase = kieContainer.getKieBase()

    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 18)
    )
    val columns = Seq("name", "age")
    val df = spark.createDataFrame(data).toDF(columns: _*)
    df.show()

    def processPartition(iterator: Iterator[Row]): Iterator[Row] = {
//      val kieSession = kieBase.newKieSession()
      try {
        iterator.map { row =>
          val name = row.getAs[String]("name")
          val age = row.getAs[Int]("age")
          val person = Person(name, age)
          try {
            val kieSession = kieBase.newKieSession()

// add this to debug
            kieSession.addEventListener(new RuleRuntimeEventListener {
              override def objectInserted(event: ObjectInsertedEvent): Unit = println(event.getObject+" inserted")
              override def objectUpdated(event: ObjectUpdatedEvent): Unit = println(event.getObject +" updated")
              override def objectDeleted(event: ObjectDeletedEvent): Unit = println(event.getOldObject + " deleted")
            })

//            println("BEFORE CHANGE")
//            println(person)
            kieSession.insert(person)
            kieSession.fireAllRules()
//            println("AFTER CHANGE")
//            println(person)
             Row.fromSeq(Seq(person.name, person.age))
          } catch {
            case e:Exception =>
              println(e.getMessage)
              Row.fromSeq(Seq(null, 0))
          }
        }
      } catch {
        case e: Exception =>
          println(e.getMessage)
          null
      }
//      finally {
//        kieSession.destroy()
//      }
//
    }

      val resultRDD = df.rdd.mapPartitions(processPartition)
      val resultDF = spark.createDataFrame(resultRDD, df.schema)
      resultDF.show()
      spark.stop()
    }
  case class Person(name: String, age: Int)
}
