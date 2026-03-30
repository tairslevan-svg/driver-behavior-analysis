package com.driver.analysis

import com.driver.ingestion.MultiSourceReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object VoiceRpmAnalysis {
  def main(args: Array[String]): Unit = {
    // 1. 参数检查
    if (args.length < 3) {
      println("Usage: VoiceRpmAnalysis <eventsPath> <statusPath> <outputPath>")
      System.exit(1)
    }
    val eventsPath = args(0)
    val statusPath = args(1)
    val outputPath = args(2)

    // 2. 创建Spark
    val spark = SparkSession.builder()
      .appName("VoiceRpmAnalysis")
      .getOrCreate()

    import spark.implicits._

    try {
      // 3. 读取数据
      val events = MultiSourceReader.readEvents(spark, eventsPath)
      val status = MultiSourceReader.readStatus(spark, statusPath)

      // 4. 关联数据
      val joined = events.join(status, Seq("timestamp"), "inner")

      // 5. 筛选：语音指令事件
      val voiceEvents = joined
        .filter(col("event_type") === "VoiceCommand")
        .select(col("timestamp"), col("rpm"), col("event_type"))

      // 6. 统计转速
      val rpmStats = voiceEvents
        .agg(
          max("rpm").alias("max_rpm"),
          min("rpm").alias("min_rpm"),
          avg("rpm").alias("avg_rpm"),
          count("*").alias("voice_count")
        )

      // 7. 展示结果
      println("===== 语音指令与发动机转速的关系 =====")
      rpmStats.show(false)

      // 8. 保存结果
      rpmStats.write
        .option("header", true)
        .mode("overwrite")
        .csv(s"$outputPath/voice_rpm_analysis")

    } finally {
      spark.stop()
    }
  }
}