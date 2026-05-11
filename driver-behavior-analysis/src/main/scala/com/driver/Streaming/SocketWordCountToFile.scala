package com.driver.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SocketWordCountToFile {
  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "localhost"
    val port = if (args.length > 1) args(1).toInt else 9999
    val batchSeconds = if (args.length > 2) args(2).toInt else 5
    val outputDir = if (args.length > 3) args(3) else "/home/hadoop/streaming/data/wordcount"
    val checkpointDir = if (args.length > 4) args(4) else "hdfs:///tmp/week5_socket_wordcount_ckpt"

    val conf = new SparkConf()
      .setAppName("SocketWordCountToFile")
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(conf, Seconds(batchSeconds))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream(host, port)
    val counts = lines
      .flatMap(_.trim.split("\\s+"))
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreachRDD { (rdd, time: Time) =>
      val batchTime = StreamingFileSink.formatBatchTime(time.milliseconds)
      val rows = rdd
        .collect()
        .sortBy { case (word, _) => word }
        .map { case (word, count) =>
          s"batch_time=${batchTime}\tword=${word}\tcount=${count}"
        }
        .toSeq

      if (rows.nonEmpty) {
        val fileName = StreamingFileSink.batchFileName("wordcount", time.milliseconds)
        StreamingFileSink.writeBatch(outputDir, fileName, rows)
        println(s"[SocketWordCountToFile] wrote ${rows.size} rows to $outputDir/$fileName")
      } else {
        println(s"[SocketWordCountToFile] batch ${batchTime} has no data")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
