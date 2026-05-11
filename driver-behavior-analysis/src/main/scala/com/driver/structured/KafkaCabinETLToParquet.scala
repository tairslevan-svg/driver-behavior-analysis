package com.driver.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, get_json_object, length, trim}
import org.apache.spark.sql.streaming.Trigger

object KafkaCabinETLToParquet {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin"
    val outputPath = if (args.length > 2) args(2) else "file:///home/hadoop/structured/week6/cabin_events"
    val checkpointPath = if (args.length > 3) args(3) else "file:///home/hadoop/structured/week6/checkpoint/cabin_events"
    val startingOffsets = if (args.length > 4) args(4) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaCabinETLToParquet")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .load()
      .selectExpr("CAST(value AS STRING) AS value")

    val parsedDF = kafkaDF.select(
      get_json_object(col("value"), "$.vehicle_id").alias("vehicle_id"),
      get_json_object(col("value"), "$.event").alias("event"),
      get_json_object(col("value"), "$.speed").cast("double").alias("speed"),
      get_json_object(col("value"), "$.acceleration").cast("double").alias("acceleration"),
      get_json_object(col("value"), "$.ts").alias("event_time")
    )

    val cleanedDF = parsedDF
      .filter(col("vehicle_id").isNotNull && length(trim(col("vehicle_id"))) > 0)
      .filter(col("event").isNotNull && length(trim(col("event"))) > 0)
      .filter(col("speed").isNotNull)
      .filter(col("acceleration").isNotNull)
      .withColumn("processing_time", current_timestamp())

    val query = cleanedDF.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
