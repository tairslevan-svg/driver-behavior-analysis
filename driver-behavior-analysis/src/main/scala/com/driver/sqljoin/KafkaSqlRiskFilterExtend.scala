
package com.driver.sqljoin

import org.apache.spark.sql.SparkSession

object KafkaSqlRiskFilterExtend {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin_sql_filter_extend"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaSqlRiskFilterExtend")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventStream = KafkaSqlSupport.buildParsedEventStream(spark, bootstrapServers, topic, startingOffsets)
    eventStream.createOrReplaceTempView("vehicle_events")

    // 扩展后的WHERE条件：新增2条风险规则
    // 1. 加速度 < -4.0：紧急减速（比普通急刹车更剧烈的减速行为）
    // 2. 加速度 > 2.5：紧急加速（快速提速的危险驾驶行为）
    val result = spark.sql(
      """
        |SELECT vehicle_id, event, speed, acceleration, event_time
        |FROM vehicle_events
        |WHERE 
        |  speed > 120 
        |  OR event IN ('hard_brake', 'sharp_turn')
        |  OR acceleration < -4.0
        |  OR acceleration > 2.5
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
