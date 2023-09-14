package live.ashish.spark.internals

import live.ashish.test.CsvHeaderFooterSkip.spark
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.kie.api.KieBase
import org.kie.api.event.rule.{ObjectDeletedEvent, ObjectInsertedEvent, ObjectUpdatedEvent, RuleRuntimeEventListener}

import scala.collection.JavaConverters._

// this is an example of how we can encode JavaBean to for a dataframe to convert into datasets
// this was done basically to enable Drools generic processing as Drools can come up with any given bean
object JavaBeanEncoder{

  def getDatasetUsingEncoder(csvDataFrame:DataFrame): Dataset[Person] = {
    val personEncoder = Encoders.bean(classOf[Person])
    val personDataset: Dataset[Person] = csvDataFrame.as(personEncoder)
    personDataset
  }
  private def getDatasetUsingEncoderWithReflection(csvDataFrame: DataFrame, className:String): DataFrame = {
      // Dynamically load the class by name
      var dataframe: DataFrame = spark.emptyDataFrame
      val runtimeClass = Class.forName(className)
      // Check if the loaded class is a valid Java bean (implements Serializable)
//    import java.io.Serializable
      if (classOf[java.io.Serializable].isAssignableFrom(runtimeClass)) {
        // Create an encoder for the loaded class
        val encoder = Encoders.bean(runtimeClass)
        // Assume `csvDataFrame` is your DataFrame containing CSV data
        dataframe = csvDataFrame.as(encoder).toDF
      }
    dataframe
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("JavaBeanToDatasetExample")
      .master("local[*]") // Use the appropriate Spark master URL
      .getOrCreate()

    val csvFilePath = "data/csv/person.csv"
    val csvDataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)

    csvDataFrame.show(false)
    csvDataFrame.printSchema()
    // first try to get this bean class from reflection
//    val personDataset:Dataset[Person] = getDatasetUsingEncoder(csvDataFrame)
//    personDataset.show()
    val outputDF = getDatasetUsingEncoderWithReflection(csvDataFrame, "live.ashish.spark.internals.Person")
    outputDF.show()
    outputDF.printSchema()
//    val resultRDD2 = df.rdd.mapPartitions(processPartitionPerSession)
//    val resultDF2 = spark.createDataFrame(resultRDD2, df.schema)

//    val resultDS = personDataset.mapPartitions(processPartitionPerSession)
//    resultDS.show()
//    personDataset.show()
  }

  // this method can do generic processPartition on the basis of
//  def processPartitionPerSession(iterator: Iterator[Row]): Iterator[Row] = {
//    val kieSession = kieBase.newKieSession()
//    try {
//      iterator.map { row =>
//        val name = row.getAs[String]("name")
//        val age = row.getAs[Int]("age")
//        val classification = row.getAs[String]("classification")
//        val person = Person(name, age, classification)
//        try {
//          val kieSession = kieBase.newKieSession()
//          // add this to debug
//          kieSession.addEventListener(new RuleRuntimeEventListener {
//            override def objectInserted(event: ObjectInsertedEvent): Unit = println(event.getObject + " inserted")
//
//            override def objectUpdated(event: ObjectUpdatedEvent): Unit = println(event.getObject + " updated from " + event.getOldObject)
//
//            override def objectDeleted(event: ObjectDeletedEvent): Unit = println(event.getOldObject + " deleted")
//          })
//
//          kieSession.insert(person)
//          val rulesFired = kieSession.fireAllRules()
//          //          println(s"Rules fired $rulesFired")
//          Row.fromSeq(Seq(person.getName, person.getAge, person.getClassification))
//        } catch {
//          case e: Exception =>
//            println(e.getMessage)
//            Row.fromSeq(Seq(null, 0, null))
//        }
//      }
//    } catch {
//      case e: Exception =>
//        println(e.getMessage)
//        null
//    }
//    finally {
//      kieSession.dispose()
//    }
//
//  }

}
