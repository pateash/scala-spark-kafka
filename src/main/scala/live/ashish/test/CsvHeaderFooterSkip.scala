package live.ashish.test

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import live.ashish.spark.etl.InitSpark


object CsvHeaderFooterSkip extends InitSpark {

  def main(args: Array[String]) = {
    import org.apache.spark.sql.Row
    val skipHeaderLines = None.getOrElse("0").toInt
    val linesRDD = spark.sparkContext.textFile(
      "data/csv/csv_skip_header_footer/CSV_LESS_HEADER_with_footer.csv"
//      "data/csv/csv_skip_header_footer/CSV_LESS_HEADER_with_footer2.csv"
    )
    val totalCount = linesRDD.count()
    var skipFooterLines = 0
    if (totalCount > 0)
      skipFooterLines = (totalCount - 1 - Some("1").getOrElse("0").toInt).toInt
    val csvRDD = linesRDD.zipWithIndex.filter({
      case (line, idx) =>
        idx >= skipHeaderLines & idx <= skipFooterLines
    })
    val dfSchema = StructType(
      csvRDD
        .take(1)
        .map({
          case (line, idx) =>
            line.split(",")
//            line.split("\t")
        })
        .map(arr => Row.fromSeq(arr.toList))
        .take(1)(0)
        .toSeq
        .map(x => StructField(x.toString, StringType))
    )
    println(dfSchema.mkString(":"))
    val finalRDD = csvRDD
      .filter({
        case (line, idx) =>
          idx >= skipHeaderLines + 1
      })
      .map({
        case (line, idx) =>
          line.split(",")
      })
      .map(arr => Row.fromSeq(arr.toList))
    var df = spark.createDataFrame(finalRDD, dfSchema)
    var userSchema = StructType(
      Array(StructField("NAME2", StringType, true),
        StructField("CLASS3", IntegerType, true),
        StructField("other", StringType, true)
      )
    ).toArray
    val dfWithSchema=df

//    val selectCommand = df.columns.zipWithIndex.map(c => s"`cast($c) `").mkString(", ")
//     Apply select command to DataFrame
//    val selectedDF = df.selectExpr(selectCommand)


    val selectString = df.columns.zipWithIndex
      .map({
        case (c, idx) =>
          s"CAST(`$c` AS ${userSchema(idx).dataType.sql}) as `${userSchema(idx).name}`"
      })
    println(selectString)
    val output =df.selectExpr(
      selectString: _*
    )
//    df.columns.zipWithIndex.foreach({
//      case (c, idx) =>
//        df = df.withColumn(userSchema(idx).name,
//          col(c).cast(userSchema(idx).dataType)
//        )
//    })
    output.show(false)
  close
  }

}

