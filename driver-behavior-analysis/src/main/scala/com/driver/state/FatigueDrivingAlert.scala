package com.driver.state

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.functions._

// 输入数据的样例类：对应输入格式 车辆ID,状态,速度
case class VehicleInput(vehicleId: String, status: String, speed: Double)
// 状态的样例类：用于保存每个车辆的累计驾驶时长（状态持久化用）
case class DriverState(accumulatedDrivingSeconds: Int)
// 输出结果的样例类
case class FatigueAlertOutput(
                               vehicleId: String,
                               currentStatus: String,
                               currentSpeed: Double,
                               accumulatedDrivingSeconds: Int,
                               alertStatus: String
                             )

object FatigueDrivingAlert {
  // 输入数据解析函数：把输入的字符串解析成结构化数据，过滤非法输入
  private def parseLine(line: String): Option[VehicleInput] = {
    val parts = line.split(",").map(_.trim)
    if (parts.length == 3 && parts(0).nonEmpty && parts(1).nonEmpty) {
      try {
        val status = parts(1).toLowerCase
        val speed = parts(2).toDouble
        Some(VehicleInput(parts(0), status, speed))
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    // 参数解析，和之前的实验保持一致，方便你复用之前的操作流程
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999
    val batchSeconds = if (args.length > 2) args(2).toInt else 5
    val checkpointDir = if (args.length > 3) args(3) else "hdfs:///tmp/week8/fatigue_driving_alert_ckpt"
    // 疲劳驾驶告警阈值：默认7200秒=2小时，测试时可以改成更小的值比如15快速验证
    val alertThreshold = if (args.length > 4) args(4).toInt else 7200

    val conf = new SparkConf()
      .setAppName("FatigueDrivingAlert")
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    // 创建SparkSession，这是Structured Streaming的入口
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // 1. 从Socket读取流数据，和之前的实验完全兼容，你可以用同样的nc命令输入数据
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // 2. 解析输入，过滤非法数据
    val inputDS = lines.as[String]
      .flatMap(parseLine)

    // 3. 有状态处理：按车辆ID分组，维护每个车辆的累计驾驶状态
    // 这部分实现了状态的保存与更新，满足实验的状态保存要求
    val alertStream = inputDS
      .groupByKey(_.vehicleId)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())((vehicleId: String, inputs: Iterator[VehicleInput], state: GroupState[DriverState]) => {
        // 读取之前保存的状态，第一次运行时初始化为0
        var currentAccumulated = state.getOption.map(_.accumulatedDrivingSeconds).getOrElse(0)

        // 处理当前批次的输入数据
        val input = inputs.next()
        val currentStatus = input.status
        val currentSpeed = input.speed

        // 更新累计时长：行驶状态累加时长，休息状态重置时长
        if (currentStatus == "driving") {
          currentAccumulated += batchSeconds
        } else {
          currentAccumulated = 0
        }

        // 判断是否触发告警：累计驾驶超过阈值，触发疲劳驾驶告警
        val alertStatus = if (currentAccumulated >= alertThreshold) "TRIGGERED" else "WAITING"

        // 更新状态，将新的累计时长保存到Checkpoint，实现故障恢复
        state.update(DriverState(currentAccumulated))

        // 输出结果
        FatigueAlertOutput(
          vehicleId,
          currentStatus,
          currentSpeed,
          currentAccumulated,
          alertStatus
        )
      })

    // 4. 启动流查询，输出结果到控制台
    val query = alertStream.writeStream
      .outputMode("update")
      .format("console")
      .option("checkpointLocation", checkpointDir)
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(s"${batchSeconds} seconds"))
      .start()

    query.awaitTermination()
  }
}