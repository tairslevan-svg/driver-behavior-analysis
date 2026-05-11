package com.driver.aggregation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WeeklyAnalyzer {
  // 分析工作日vs周末的事件分布
  def weekdayWeekendEvents(df: DataFrame): DataFrame = {
    df.withColumn("weekday_num", dayofweek(col("timestamp")))  // 1=周日, 2=周一,...,7=周六
      .withColumn("day_type",
        when(col("weekday_num").between(2, 6), "weekday")  // 周一~周五=工作日
          .otherwise("weekend")                           // 周六、周日=周末
      )
      .groupBy("day_type")
      .agg(
        sum("is_hard_accel").as("hard_accel_count"),
        sum("is_hard_decel").as("hard_decel_count"),
        sum("is_hard_turn").as("hard_turn_count"),
        count("*").as("total_records"),  // 总记录数（用于计算事件发生率）
        // 计算各类事件发生率（每千条记录的事件数）
        round(sum("is_hard_accel") * 1000 / count("*"), 2).as("accel_rate_per_1000"),
        round(sum("is_hard_decel") * 1000 / count("*"), 2).as("decel_rate_per_1000"),
        round(sum("is_hard_turn") * 1000 / count("*"), 2).as("turn_rate_per_1000")
      )
      .orderBy("day_type")
  }

  // 扩展：按星期几（周一到周日）详细统计
  def dailyEvents(df: DataFrame): DataFrame = {
    df.withColumn("weekday_num", dayofweek(col("timestamp")))
      .withColumn("weekday_name",
        when(col("weekday_num") === 1, "周日")
          .when(col("weekday_num") === 2, "周一")
          .when(col("weekday_num") === 3, "周二")
          .when(col("weekday_num") === 4, "周三")
          .when(col("weekday_num") === 5, "周四")
          .when(col("weekday_num") === 6, "周五")
          .when(col("weekday_num") === 7, "周六")
      )
      .groupBy("weekday_num", "weekday_name")
      .agg(
        sum("is_hard_accel").as("hard_accel_count"),
        sum("is_hard_decel").as("hard_decel_count"),
        sum("is_hard_turn").as("hard_turn_count")
      )
      .orderBy("weekday_num")
  }
}
