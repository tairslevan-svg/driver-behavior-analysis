
package com.driver.sqljoin

import org.apache.spark.sql.SparkSession

/**
 * 自定义新实验：各驾驶事件平均速度实时统计
 */
object KafkaSqlEventAvgSpeed {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin_sql_avg_speed"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaSqlEventAvgSpeed")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventStream = KafkaSqlSupport.buildParsedEventStream(spark, bootstrapServers, topic, startingOffsets)
    eventStream.createOrReplaceTempView("vehicle_events")

    // 统计每种事件的平均速度、事件总数
    val result = spark.sql(
      """
        |SELECT
        |  event,
        |  ROUND(AVG(speed), 2) AS avg_speed,
        |  COUNT(*) AS event_count
        |FROM vehicle_events
        |GROUP BY event
        |ORDER BY avg_speed DESC
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
