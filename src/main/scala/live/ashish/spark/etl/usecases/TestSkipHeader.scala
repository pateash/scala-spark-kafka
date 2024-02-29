package live.ashish.spark.etl.usecases

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import live.ashish.spark.etl.InitSpark
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object TestSkipHeader extends InitSpark {
  val path = "data/csv/csv_skip_header_footer/skip-Header_footer_empty.csv"
  val skipHeaders = Some("1")
  val skipFooters = Some("1")
  val useSchema = Some(true)
  val separator = Some(",")
  val schema = Some(StructType(Array(
    StructField("col1", StringType),
    StructField("col2", IntegerType)
  )
  ))

  def main(args: Array[String]) = {

    //    val sc = spark.sparkContext
    //    val  rdds = sc.textFile("data/csv/csv_skip_header_footer/skip-Header_footer_empty.csv")
    //    println(rdds.collect().mkString(","))
    val df = solve()
    df.show(false)
    close
  }


  def solve(): DataFrame = {

    import org.apache.spark.sql.Row
    val skipHeaderLines = skipHeaders.getOrElse("0").toInt

    val linesRDD = spark.sparkContext.textFile(path) // Read file as RDD of lines
    val totalCount = linesRDD.count()
    var skipFooterLines = 0
    if (totalCount > 0) {
      skipFooterLines = (totalCount - 1 - skipFooters.getOrElse("0").toInt).toInt
    }

    val csvRDD = linesRDD.zipWithIndex // Add line number to each line
      .filter {
        case (line, idx) => (idx >= skipHeaderLines) & (idx <= skipFooterLines)
      } // Filter out first n lines

    val finalRDD = csvRDD
      .map {
        case (line, idx) =>
          if (useSchema.isDefined && useSchema.get)
            line.split(separator.get).padTo(schema.get.toArray.length, null)
          else line.split(separator.get)
      } // Split each line into array of values
      .map(arr => Row.fromSeq(arr.toList)) // Convert each array to a Row with a serializable collection

    val schemaSize = if(useSchema.isDefined && useSchema.get) schema.get.toArray.length else finalRDD.first.size
    val newSchema = StructType(
          Array.tabulate(schemaSize)(i => StructField(s"col$i", StringType))
    ) // Infer schema based on first row

    if (useSchema.isDefined && useSchema.get) {
      val df = spark.createDataFrame(finalRDD, newSchema)
      var userSchema = schema.get.toArray
      spark.createDataFrame(finalRDD, newSchema)
      val selectCommands = df.columns.zipWithIndex.map {
        case (c, idx) => s"CAST(`col$idx` AS ${userSchema(idx).dataType.sql}) as `${userSchema(idx).name}`"
      }
//       Apply select command to DataFrame
      df.selectExpr(selectCommands: _*)
    } else {
      spark.createDataFrame(finalRDD, newSchema)
    }

  }
}
