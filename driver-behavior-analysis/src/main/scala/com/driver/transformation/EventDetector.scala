package com.driver.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object EventDetector {
  def addEventFlags(df: DataFrame, accelTh: Double, decelTh: Double, lateralTh: Double): DataFrame = {
    df.withColumn("is_hard_accel", when(col("acceleration") > accelTh, 1).otherwise(0))
      .withColumn("is_hard_decel", when(col("acceleration") < decelTh, 1).otherwise(0))
      .withColumn("is_hard_turn", when(col("lateral_acceleration") > lateralTh, 1).otherwise(0))
  }
}
