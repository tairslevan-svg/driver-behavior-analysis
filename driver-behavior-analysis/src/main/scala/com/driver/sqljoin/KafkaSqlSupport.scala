
package com.driver.sqljoin

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, from_json}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object KafkaSqlSupport {
  val vehicleEventSchema: StructType = StructType(Seq(
    StructField("vehicle_id", StringType, nullable = true),
    StructField("event", StringType, nullable = true),
    StructField("speed", DoubleType, nullable = true),
    StructField("acceleration", DoubleType, nullable = true),
    StructField("ts", StringType, nullable = true)
  ))

  def buildParsedEventStream(
      spark: SparkSession,
      bootstrapServers: String,
      topic: String,
      startingOffsets: String
  ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), vehicleEventSchema).alias("data"))
      .select(
        col("data.vehicle_id").alias("vehicle_id"),
        col("data.event").alias("event"),
        col("data.speed").alias("speed"),
        col("data.acceleration").alias("acceleration"),
        col("data.ts").alias("event_time")
      )
      .withColumn("processing_time", current_timestamp())
  }

  def buildVehicleDimension(spark: SparkSession): DataFrame = {
    import spark.implicits._

    Seq(
      ("V001", "张三", "一队"),
      ("V002", "李四", "一队"),
      ("V003", "王五", "二队"),
      ("V004", "赵六", "二队"),
      ("V005", "孙七", "三队"),
      ("V006", "周八", "三队")
    ).toDF("vehicle_id", "driver_name", "fleet_name")
  }
}
