package com.driver.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, get_json_object}

object KafkaJsonParserToConsole {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaJsonParserToConsole")
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
      get_json_object(col("value"), "$.ts").alias("event_time"),
      current_timestamp().alias("processing_time")
    )

    val query = parsedDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .start()

    query.awaitTermination()
  }
}
