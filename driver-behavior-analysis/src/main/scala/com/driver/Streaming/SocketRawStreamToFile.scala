package com.driver.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SocketRawStreamToFile {
  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999
    val batchSeconds = if (args.length > 2) args(2).toInt else 5
    val outputDir = if (args.length > 3) args(3) else "/home/hadoop/streaming/data/raw"
    val checkpointDir = if (args.length > 4) args(4) else "hdfs:///tmp/week5_socket_raw_ckpt"

    val conf = new SparkConf()
      .setAppName("SocketRawStreamToFile")
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream(host, port)

    lines.foreachRDD { (rdd, time: Time) =>
      val batchTime = StreamingFileSink.formatBatchTime(time.milliseconds)
      val rows = rdd.collect().zipWithIndex.map { case (line, index) =>
        s"batch_time=${batchTime}\trecord_no=${index + 1}\traw_data=${line}"
      }.toSeq

      if (rows.nonEmpty) {
        val fileName = StreamingFileSink.batchFileName("raw_stream", time.milliseconds)
        StreamingFileSink.writeBatch(outputDir, fileName, rows)
        println(s"[SocketRawStreamToFile] wrote ${rows.size} records to $outputDir/$fileName")
      } else {
        println(s"[SocketRawStreamToFile] batch ${batchTime} has no data")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
