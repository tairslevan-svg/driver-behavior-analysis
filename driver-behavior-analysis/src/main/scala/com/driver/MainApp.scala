package com.driver
import com.driver.Config.AppConfig
import com.driver.ingestion.DataReader
import com.driver.transformation.EventDetector
import com.driver.aggregation.{TimeAggregator, VehicleScorer, WeeklyAnalyzer}  // 新增WeeklyAnalyzer
import com.driver.output.DataWriter
import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {
    val config = AppConfig.parse(args)

    val spark = SparkSession.builder()
      .appName("DriverBehaviorAnalysis")
      .getOrCreate()

    try {
      // 1. 读取数据
      val rawDF = DataReader.readCSV(spark, config.inputPath)

      // 2. 添加事件标志
      val eventDF = EventDetector.addEventFlags(
        rawDF,
        config.accelThreshold,
        config.decelThreshold,
        config.lateralThreshold
      )

      // 3. 原有分析：每小时事件统计
      val hourlyResult = TimeAggregator.hourlyEvents(eventDF)
      DataWriter.writeCSV(hourlyResult, s"${config.outputPath}/hourly_events")
      DataWriter.writeParquet(hourlyResult, s"${config.outputPath}/hourly_events_parquet")

      // 4. 原有分析：车辆评分（已替换为指数衰减模型）
      val vehicleScores = VehicleScorer.vehicleScores(eventDF)
      DataWriter.writeCSV(vehicleScores, s"${config.outputPath}/vehicle_scores")
      DataWriter.writeParquet(vehicleScores, s"${config.outputPath}/vehicle_scores_parquet")

      // 5. 新增分析：工作日vs周末事件分布
      val weeklyResult = WeeklyAnalyzer.weekdayWeekendEvents(eventDF)
      DataWriter.writeCSV(weeklyResult, s"${config.outputPath}/weekday_weekend_events")
      DataWriter.writeParquet(weeklyResult, s"${config.outputPath}/weekday_weekend_parquet")

      // 6. 扩展分析：按星期几详细统计
      val dailyResult = WeeklyAnalyzer.dailyEvents(eventDF)
      DataWriter.writeCSV(dailyResult, s"${config.outputPath}/daily_events")
      DataWriter.writeParquet(dailyResult, s"${config.outputPath}/daily_events_parquet")

      println("All analysis completed successfully.")
    } finally {
      spark.stop()
    }
  }
}