package com.poc.drools.spark

import com.poc.drools.spark.util.Utils._
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object CustomStructUtils {

  def prepTrafficStatsStruct(inputDf: DataFrame): DataFrame = {
    inputDf.withColumn("traffic", struct(col("traffic_light").as("light"), col("cid").cast(IntegerType).as("cid")))
      .withColumn("style", struct(col("driving_style").as("style")))
      .withColumn("response", struct(lit("").as("action"))) //initializing with empty response
  }

  def prepTrafficStatsObj(row: Row): TrafficStats = {
    val traffic = Traffic(row.getString(1), row.getString(0).toInt)
    val style = DrivingStyle(row.getString(2))
    val response = TrafficResponse(null)

    TrafficStats(traffic, style, response)
  }

  def prepTrafficStatsStructType(): StructType = {
    StructType(Array(StructField("cid", IntegerType, true),
      StructField("traffic_light", StringType, true),
      StructField("driving_style", StringType, true),
      StructField("response", StringType, true)))
  }
}
