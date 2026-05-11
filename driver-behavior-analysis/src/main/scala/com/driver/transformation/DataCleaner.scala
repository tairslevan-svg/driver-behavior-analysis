package com.driver.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean}

/**
 * 传感器数据清洗工具类
 */
object DataCleaner {

  def readSensors(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(path)
      .withColumn("temp", col("temp").cast("double"))
      .withColumn("speed", col("speed").cast("int"))
      .withColumn("humidity", col("humidity").cast("double"))
      .withColumn("pressure", col("pressure").cast("double"))
  }

  def fillMissingTemp(df: DataFrame): DataFrame = {
    val meanTempRow = df.select(mean("temp")).first()
    val meanTemp = if (meanTempRow.isNullAt(0)) 0.0 else meanTempRow.getDouble(0)
    println(s"温度均值: $meanTemp")
    df.na.fill(meanTemp, Seq("temp"))
  }

  def filterAbnormalSpeed(df: DataFrame, maxSpeed: Int = 200): DataFrame = {
    df.filter(col("speed") < maxSpeed)
  }

  // 修复后的 clean 方法
  def clean(df: DataFrame): DataFrame = {
    df.transform(fillMissingTemp)
      .transform(filterAbnormalSpeed(_, 200)) // ✅ 修复点
  }
}