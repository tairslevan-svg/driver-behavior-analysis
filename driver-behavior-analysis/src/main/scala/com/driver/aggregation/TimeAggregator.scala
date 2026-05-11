package com.driver.aggregation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TimeAggregator {
  def hourlyEvents(df: DataFrame): DataFrame = {
    df.groupBy(hour(col("timestamp")).as("hour"))
      .agg(
        sum("is_hard_accel").as("hard_accel_count"),
        sum("is_hard_decel").as("hard_decel_count"),
        sum("is_hard_turn").as("hard_turn_count"),
        count("*").as("total_records")
      )
      .orderBy("hour")
  }
}