
package com.driver.sqljoin

import org.apache.spark.sql.SparkSession

object KafkaSqlRiskFilterToConsole {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin_sql"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaSqlRiskFilterToConsole")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventStream = KafkaSqlSupport.buildParsedEventStream(spark, bootstrapServers, topic, startingOffsets)
    eventStream.createOrReplaceTempView("vehicle_events")

    val result = spark.sql(
      """
        |SELECT vehicle_id, event, speed, acceleration, event_time
        |FROM vehicle_events
        |WHERE speed > 120 OR event IN ('hard_brake', 'sharp_turn')
      """.stripMargin)

    val query = result.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .start()

    query.awaitTermination()
  }
}
