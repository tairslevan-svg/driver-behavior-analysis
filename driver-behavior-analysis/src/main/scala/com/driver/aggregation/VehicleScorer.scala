package com.driver.aggregation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object VehicleScorer {
  def vehicleScores(df: DataFrame): DataFrame = {
    df.groupBy("vehicle_id")
      .agg(
        sum("is_hard_accel").as("accel_count"),
        sum("is_hard_decel").as("decel_count"),
        sum("is_hard_turn").as("turn_count")
      )
      .withColumn("risk_score",
        round(
          expr("100 * exp(- (accel_count * 0.02 + decel_count * 0.03 + turn_count * 0.015))"),
          2
        )
      )
      .withColumn("risk_score", greatest(col("risk_score"), lit(0)))
      .orderBy(desc("risk_score"))
  }
}