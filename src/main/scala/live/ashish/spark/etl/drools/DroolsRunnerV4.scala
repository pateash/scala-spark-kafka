package live.ashish.spark.etl.drools

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.kie.api.KieBase
import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}
import org.kie.internal.io.ResourceFactory
import org.kie.internal.utils.KieHelper

// use jdk8
// Using generic Drools Processor
// this takes bean class name and rule path and runs it
object DroolsRunnerV4 {
  val ruleFilePath = "rules/person_rules3.drl"
   val beanClassName = "live.ashish.spark.internals.People"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DroolsRuleEngineV4")
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
    val df = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/drools-input/People.csv")
     val encoder = Encoders.bean(Class.forName(beanClassName))
    df.show()
    df.printSchema()
    val resultRDD2 = df.rdd.mapPartitions(processPartitionPerSession2)
    val resultDF2 = spark.createDataFrame(resultRDD2, df.schema)
    resultDF2.show()
    spark.stop()
    }

  private def createBeanInstance(beanClassName: String, row: Row): Any = {
    try {
      // Load the bean class using reflection
      val beanClass = Class.forName(beanClassName)

      // Get the constructor of the bean class (assuming it has a default constructor)
      val constructor = beanClass.getDeclaredConstructor()

      // Create a new instance of the bean
      val beanInstance = constructor.newInstance()

      // Assuming that the DataFrame columns correspond to bean fields by name
      val fields = beanClass.getDeclaredFields

      for (field <- fields) {
        field.setAccessible(true)
        val fieldName = field.getName

        // Assuming that the column names in the DataFrame match the field names of the bean
        val columnIndex = row.fieldIndex(fieldName)

        if (columnIndex != -1) {
          val columnValue = row.get(columnIndex)

          // Convert the columnValue to the appropriate data type if needed
          // For simplicity, we assume that the types match here
          field.set(beanInstance, columnValue)
        }
      }
      beanInstance
    } catch {
      case ex: Exception =>
        throw new RuntimeException("Error creating bean instance using reflection.", ex)
    }
  }
  def processPartitionPerSession2(iterator: Iterator[Row])(implicit kieBase: KieBase): Iterator[Row] = {
    val kieSession = kieBase.newKieSession()
    try {
      iterator.map { row =>
        val beanInstance = createBeanInstance(beanClassName, row)
        try {
          val kieSession = kieBase.newKieSession()
//          kieSession.addEventListener(new RuleRuntimeEventListener {
//            override def objectInserted(event: ObjectInsertedEvent): Unit = println(event.getObject + " inserted")
//            override def objectUpdated(event: ObjectUpdatedEvent): Unit = println(event.getObject + " updated from " + event.getOldObject)
//            override def objectDeleted(event: ObjectDeletedEvent): Unit = println(event.getOldObject + " deleted")
//          })
          kieSession.insert(beanInstance)
         val rulesFired = kieSession.fireAllRules()
          println(s"Rules fired $rulesFired")

          val fields = beanInstance.getClass.getDeclaredFields
          // Create a sequence of values from the bean instance
          val values = fields.map { field =>
            field.setAccessible(true)
            field.get(beanInstance)
          }
          Row.fromSeq(values)
        } catch {
          case e: Exception =>
            println(e.getMessage)
            Row.fromSeq(Seq())
        }
      }
    } catch {
      case e: Exception =>
        println(e.getMessage)
        null
    }
    finally {
      kieSession.dispose()
    }

  }


}
