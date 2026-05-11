package com.driver.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.util.Try

object WindowBrakeCountToFile {
  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999
    val batchSeconds = if (args.length > 2) args(2).toInt else 5
    val windowSeconds = if (args.length > 3) args(3).toInt else 60
    val slideSeconds = if (args.length > 4) args(4).toInt else 10
    val brakeThreshold = if (args.length > 5) args(5).toDouble else -5.0
    val outputDir = if (args.length > 6) args(6) else "/home/hadoop/streaming/data/brake_window"
    val checkpointDir = if (args.length > 7) args(7) else "hdfs:///tmp/week5_brake_window_ckpt"

    require(windowSeconds % batchSeconds == 0, "windowSeconds must be a multiple of batchSeconds")
    require(slideSeconds % batchSeconds == 0, "slideSeconds must be a multiple of batchSeconds")

    val conf = new SparkConf()
      .setAppName("WindowBrakeCountToFile")
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream(host, port)
    val values = lines.flatMap { line =>
      line.trim.split("\\s+").flatMap(item => Try(item.toDouble).toOption)
    }

    val brakeEvents = values
      .filter(_ < brakeThreshold)
      .map(_ => ("hard_brake", 1))

    val windowCounts = brakeEvents.reduceByKeyAndWindow(
      (left: Int, right: Int) => left + right,
      Seconds(windowSeconds),
      Seconds(slideSeconds)
    )

    windowCounts.foreachRDD { (rdd, time: Time) =>
      val batchTime = StreamingFileSink.formatBatchTime(time.milliseconds)
      val rows = rdd.collect().map { case (metric, count) =>
        s"batch_time=${batchTime}\tmetric=${metric}\tcount=${count}\twindow_seconds=${windowSeconds}\tslide_seconds=${slideSeconds}\tthreshold=${brakeThreshold}"
      }.toSeq

      if (rows.nonEmpty) {
        val fileName = StreamingFileSink.batchFileName("brake_window", time.milliseconds)
        StreamingFileSink.writeBatch(outputDir, fileName, rows)
        println(s"[WindowBrakeCountToFile] wrote ${rows.size} rows to $outputDir/$fileName")
      } else {
        println(s"[WindowBrakeCountToFile] batch ${batchTime} has no brake events in the current window")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
