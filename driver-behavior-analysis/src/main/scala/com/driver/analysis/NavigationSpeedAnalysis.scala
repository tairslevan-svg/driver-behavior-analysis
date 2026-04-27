package com.driver.analysis

import com.driver.ingestion.MultiSourceReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NavigationSpeedAnalysis {
  def main(args: Array[String]): Unit = {
    // 1. 参数校验
    if (args.length < 3) {
      println("Usage: NavigationSpeedAnalysis <eventsPath> <statusPath> <outputPath>")
      System.exit(1)
    }
    val eventsPath = args(0)
    val statusPath = args(1)
    val outputPath = args(2)

    // 2. 初始化Spark
    val spark = SparkSession.builder()
      .appName("NavigationSpeedAnalysis")
      .getOrCreate()

    try {
      // 3. 复用项目已有Reader读取多源数据
      val events = MultiSourceReader.readEvents(spark, eventsPath)
      val status = MultiSourceReader.readStatus(spark, statusPath)

      // 4. 按时间戳关联事件与车辆状态
      val joined = events.join(status, Seq("timestamp"), "inner")

      // 5. 筛选出「导航点击」事件
      val navEvents = joined
        .filter(col("event_type") === "NavigationStart")
        .select("timestamp", "speed")

      // 6. 分桶统计车速分布
      val speedStats = navEvents
        .groupBy(
          when(col("speed") < 30, "0-30km/h")
            .when(col("speed") < 60, "30-60km/h")
            .when(col("speed") < 90, "60-90km/h")
            .otherwise("90km/h+")
            .alias("speed_range")
        )
        .count()
        .orderBy("speed_range")

      // 7. 输出结果
      println("===== 用户点击导航时的车速分布 =====")
      speedStats.show(false)

      // 8. 保存结果到输出路径
      speedStats.write
        .option("header", true)
        .mode("overwrite")
        .csv(s"$outputPath/navigation_speed_distribution")
    } finally {
      spark.stop()
    }
  }
}