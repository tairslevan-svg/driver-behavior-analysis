package com.driver.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object LatestVehicleStateToConsole {
  private def parseLine(line: String): Option[(String, String)] = {
    val parts = line.split(",").map(_.trim)
    if (parts.length == 2 && parts(0).nonEmpty && parts(1).nonEmpty) {
      Some(parts(0) -> parts(1).toLowerCase)
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999
    val batchSeconds = if (args.length > 2) args(2).toInt else 5
    val checkpointDir = if (args.length > 3) args(3) else "hdfs:///tmp/week7/latest_vehicle_state_ckpt"

    val conf = new SparkConf()
      .setAppName("LatestVehicleStateToConsole")
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream(host, port)
    val statusPairs = lines.flatMap(parseLine)

    val updateFunction = (vehicleId: String, currentStatus: Option[String], state: State[String]) => {
      val latestStatus = currentStatus.getOrElse(state.getOption.getOrElse("unknown"))
      state.update(latestStatus)
      s"vehicle_id=$vehicleId\tlatest_state=$latestStatus"
    }

    val updates = statusPairs.mapWithState(StateSpec.function(updateFunction))

    updates.foreachRDD { rdd =>
      if (rdd.isEmpty()) {
        println("[LatestVehicleStateToConsole] no data in this batch")
      } else {
        println("[LatestVehicleStateToConsole] batch result:")
        rdd.collect().sorted.foreach(println)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}