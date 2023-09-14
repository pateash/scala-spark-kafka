package live.ashish.spark.internals

import live.ashish.test.CsvHeaderFooterSkip.spark
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.kie.api.KieBase
import org.kie.internal.io.ResourceFactory
import org.kie.internal.utils.KieHelper

// this is an example of how we can encode JavaBean to for a dataframe to convert into datasets
// this was done basically to enable Drools generic processing as Drools can come up with any given bean
object JavaBeanEncoder {

  def getDatasetUsingEncoder(csvDataFrame: DataFrame): Dataset[Person] = {
    val personEncoder = Encoders.bean(classOf[Person])
    val personDataset: Dataset[Person] = csvDataFrame.as(personEncoder)
    personDataset
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

    val kieHelper = new KieHelper()
    val ruleFilePath = "rules/classification/person_rules2.drl"
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

    csvDataFrame.show(false)
    csvDataFrame.printSchema()

    val beanClassName = "live.ashish.spark.internals.Person"
    val runtimeClass = Class.forName(beanClassName)
    if (classOf[java.io.Serializable].isAssignableFrom(runtimeClass)) {
      // Create an encoder for the loaded class
      val encoder = Encoders.bean(runtimeClass)
      // Assume `csvDataFrame` is your DataFrame containing CSV data
      val dataset = csvDataFrame.as(encoder)
      dataset.show()
      dataset.printSchema()

      //      dataset.mapPartitions(EncoderUtils.processPartition[ClassTag(classOf[])], encoder)
      //    val processedDF = dataset.mapPartitions { iter =>
      //      val constructor = _ // Obtain the constructor for MyBean through Java reflection
      //        processPartition(iter, constructor)
      //    }

    }
    else {
      println("Bean is not serializable")
    }

    //    //    val outputDF = getDataframeUsingEncoderWithReflection(csvDataFrame, "live.ashish.spark.internals.Person")
    //    val outputDF = getDatasetUsingEncoderWithReflection(csvDataFrame, "live.ashish.spark.internals.Person")
    //    outputDF.show()
    //    outputDF.printSchema()


    //    val resultRDD2 = df.rdd.mapPartitions(processPartitionPerSession)
    //    val resultDF2 = spark.createDataFrame(resultRDD2, df.schema)

    //    val resultDS = personDataset.mapPartitions(processPartitionPerSession)
    //    resultDS.show()
    //    personDataset.show()
  }

  private def getDataframeUsingEncoderWithReflection(csvDataFrame: DataFrame, className: String): DataFrame = {
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
}
