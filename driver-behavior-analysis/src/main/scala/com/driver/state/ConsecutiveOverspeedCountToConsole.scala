package com.driver.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object ConsecutiveOverspeedCountToConsole {
  private def parseLine(line: String): Option[(String, Double)] = {
    val parts = line.split(",").map(_.trim)
    if (parts.length == 2 && parts(0).nonEmpty) {
      try {
        Some(parts(0) -> parts(1).toDouble)
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999
    val batchSeconds = if (args.length > 2) args(2).toInt else 5
    val checkpointDir = if (args.length > 3) args(3) else "hdfs:///tmp/week7/consecutive_overspeed_count_ckpt"
    val overspeedThreshold = if (args.length > 4) args(4).toDouble else 120.0

    val conf = new SparkConf()
      .setAppName("ConsecutiveOverspeedCountToConsole")
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream(host, port)
    val speedPairs = lines.flatMap(parseLine)

    val updateFunction = (vehicleId: String, speedOption: Option[Double], state: State[Int]) => {
      val speed = speedOption.getOrElse(0.0)
      val previousCount = state.getOption().getOrElse(0)
      val newCount = if (speed > overspeedThreshold) previousCount + 1 else 0
      state.update(newCount)
      s"vehicle_id=$vehicleId\tspeed=$speed\tconsecutive_overspeed_count=$newCount"
    }

    val updates = speedPairs.mapWithState(StateSpec.function(updateFunction))

    updates.foreachRDD { rdd =>
      if (rdd.isEmpty()) {
        println("[ConsecutiveOverspeedCountToConsole] no data in this batch")
      } else {
        println("[ConsecutiveOverspeedCountToConsole] batch result:")
        rdd.collect().sorted.foreach(println)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}