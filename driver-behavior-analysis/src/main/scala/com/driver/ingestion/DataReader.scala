package com.driver.ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataReader {
  def readCSV(spark: SparkSession, path: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .csv(path)
      .withColumn("timestamp", to_timestamp(col("timestamp")))
      .withColumn("speed", col("speed").cast("double"))
      .withColumn("acceleration", col("acceleration").cast("double"))
      .withColumn("lateral_acceleration", col("lateral_acceleration").cast("double"))
    df.cache()   // 缓存数据，后续多次使用
  }
}