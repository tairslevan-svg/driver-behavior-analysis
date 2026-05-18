
package com.driver.sqljoin

import org.apache.spark.sql.SparkSession

object KafkaSqlStaticJoinExtend {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val topic = if (args.length > 1) args(1) else "cabin_sql_join_extend"
    val startingOffsets = if (args.length > 2) args(2) else "latest"

    val spark = SparkSession.builder()
      .appName("KafkaSqlStaticJoinExtend")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventStream = KafkaSqlSupport.buildParsedEventStream(spark, bootstrapServers, topic, startingOffsets)
    // 扩展后的静态维表：新增plate_no（车牌）、driver_level（司机等级）两个字段
    val vehicleDim = buildExtendVehicleDimension(spark)

    eventStream.createOrReplaceTempView("vehicle_events")
    vehicleDim.createOrReplaceTempView("vehicle_dim")

    val result = spark.sql(
      """
        |SELECT
        |  e.vehicle_id,
        |  d.driver_name,
        |  d.fleet_name,
        |  d.plate_no,
        |  d.driver_level,
        |  e.event,
        |  e.speed,
        |  e.event_time
        |FROM vehicle_events e
        |LEFT JOIN vehicle_dim d
        |ON e.vehicle_id = d.vehicle_id
      """.stripMargin)

    val query = result.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .start()

    query.awaitTermination()
  }

  // 扩展后的维表构建方法，新增2个字段
  def buildExtendVehicleDimension(spark: SparkSession): DataFrame = {
    import spark.implicits._

    Seq(
      ("V001", "张三", "一队", "京A12345", "A级"),
      ("V002", "李四", "一队", "京B67890", "B级"),
      ("V003", "王五", "二队", "沪A11223", "A级"),
      ("V004", "赵六", "二队", "沪C44556", "C级"),
      ("V005", "孙七", "三队", "粤A77889", "B级"),
      ("V006", "周八", "三队", "粤D00112", "A级")
    ).toDF("vehicle_id", "driver_name", "fleet_name", "plate_no", "driver_level")
  }
}
