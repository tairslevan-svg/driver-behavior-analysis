package com.driver.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, get_json_object, length, trim}

object KafkaEventCountToConsole {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaEventCountToConsole")
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
      get_json_object(col("value"), "$.event").alias("event")
    ).filter(col("event").isNotNull && length(trim(col("event"))) > 0)

    val eventCountDF = parsedDF
      .groupBy("event")
      .count()
      .orderBy(col("count").desc, col("event").asc)

    val query = eventCountDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .start()

    query.awaitTermination()
  }
}
