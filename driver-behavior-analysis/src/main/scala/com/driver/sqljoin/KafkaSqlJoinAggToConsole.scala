
package com.driver.sqljoin

import org.apache.spark.sql.SparkSession

object KafkaSqlJoinAggToConsole {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin_sql"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaSqlJoinAggToConsole")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventStream = KafkaSqlSupport.buildParsedEventStream(spark, bootstrapServers, topic, startingOffsets)
    val vehicleDim = KafkaSqlSupport.buildVehicleDimension(spark)

    eventStream.createOrReplaceTempView("vehicle_events")
    vehicleDim.createOrReplaceTempView("vehicle_dim")

    val result = spark.sql(
      """
        |SELECT
        |  COALESCE(d.fleet_name, '未知车队') AS fleet_name,
        |  e.event,
        |  COUNT(*) AS event_count
        |FROM vehicle_events e
        |LEFT JOIN vehicle_dim d
        |ON e.vehicle_id = d.vehicle_id
        |GROUP BY COALESCE(d.fleet_name, '未知车队'), e.event
        |ORDER BY fleet_name, event_count DESC, e.event
      """.stripMargin)

    val query = result.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .start()

    query.awaitTermination()
  }
}
