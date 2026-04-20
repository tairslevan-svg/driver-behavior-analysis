package com.driver.jobs

import com.driver.ingestion.MultiSourceReader
import com.driver.output.DataWriter
import org.apache.spark.sql.SparkSession

object MultiSourceJob {
  def main(args: Array[String]): Unit = {
    // 校验输入参数
    if (args.length < 3) {
      println("Usage: MultiSourceJob <eventsPath> <statusPath> <outputPath>")
      System.exit(1)
    }

    // 解析输入参数
    val eventsPath = args(0)
    val statusPath = args(1)
    val outputPath = args(2)

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("MultiSourceJoin")
      .getOrCreate()

    try {
      // 读取并关联数据
      val events = MultiSourceReader.readEvents(spark, eventsPath)
      val status = MultiSourceReader.readStatus(spark, statusPath)
      val joined = MultiSourceReader.joinEventsAndStatus(events, status)

      // 查看前20条数据
      println("=== 关联后的数据（前20行）===")
      joined.show(20, truncate = false)
      println(s"总行数: ${joined.count()}")

      // 写入结果（CSV + Parquet）
      DataWriter.writeCSV(joined, s"$outputPath/joined_events_status")
      DataWriter.writeParquet(joined, s"$outputPath/joined_events_status_parquet")

      println(s"结果已写入 HDFS: $outputPath")
    } finally {
      // 关闭SparkSession
      spark.stop()
    }
  }
}
