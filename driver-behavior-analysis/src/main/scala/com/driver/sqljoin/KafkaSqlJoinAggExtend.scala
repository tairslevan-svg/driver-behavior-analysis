
package com.driver.sqljoin

import org.apache.spark.sql.SparkSession

object KafkaSqlJoinAggExtend {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin_sql_agg_extend"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaSqlJoinAggExtend")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventStream = KafkaSqlSupport.buildParsedEventStream(spark, bootstrapServers, topic, startingOffsets)
    val vehicleDim = KafkaSqlSupport.buildVehicleDimension(spark)

    eventStream.createOrReplaceTempView("vehicle_events")
    vehicleDim.createOrReplaceTempView("vehicle_dim")

    // 新的统计口径：按司机姓名统计，统计每个司机的总事件数、超速事件数
    val result = spark.sql(
      """
        |SELECT
        |  COALESCE(d.driver_name, '未知司机') AS driver_name,
        |  COUNT(*) AS total_event_count,
        |  SUM(CASE WHEN e.event = 'overspeed' THEN 1 ELSE 0 END) AS overspeed_count,
        |  ROUND(SUM(CASE WHEN e.event = 'overspeed' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS overspeed_ratio
        |FROM vehicle_events e
        |LEFT JOIN vehicle_dim d
        |ON e.vehicle_id = d.vehicle_id
        |GROUP BY COALESCE(d.driver_name, '未知司机')
        |ORDER BY total_event_count DESC
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
