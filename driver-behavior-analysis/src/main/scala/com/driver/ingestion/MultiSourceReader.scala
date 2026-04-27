package com.driver.ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object MultiSourceReader {

  // 读取JSON格式的事件数据
  def readEvents(spark: SparkSession, path: String): DataFrame = {
    spark.read.json(path)
  }

  // 读取Parquet格式的车辆状态数据
  def readStatus(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  // 通过timestamp字段内连接事件数据和车辆状态数据
  def joinEventsAndStatus(events: DataFrame, status: DataFrame): DataFrame = {
    events.join(status, Seq("timestamp"), "inner")
  }
}
