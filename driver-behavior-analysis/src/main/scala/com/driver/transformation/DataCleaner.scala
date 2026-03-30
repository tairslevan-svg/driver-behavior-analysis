package com.driver.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, mean}

object DataCleaner {

  // 读取并转换数据类型
  def cleanAndCastTypes(spark: org.apache.spark.sql.SparkSession, path: String): DataFrame = {
    var df = spark.read.option("header", "true").csv(path)
    df = df.withColumn("temp", col("temp").cast("double"))
      .withColumn("speed", col("speed").cast("int"))
      .withColumn("humidity", col("humidity").cast("double"))
      .withColumn("pressure", col("pressure").cast("double"))
    df
  }

  // 填充缺失温度
  def fillMissingTemp(df: DataFrame): DataFrame = {
    val meanTemp = df.select(mean("temp")).first().getDouble(0)
    println(s"温度均值: $meanTemp")
    df.na.fill(meanTemp, Seq("temp"))
  }

  // 过滤异常速度
  def filterAbnormalSpeed(df: DataFrame, maxSpeed: Int = 200): DataFrame = {
    df.filter(col("speed") < maxSpeed)
  }

}