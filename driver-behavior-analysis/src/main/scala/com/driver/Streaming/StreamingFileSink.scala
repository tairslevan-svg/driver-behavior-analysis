package com.driver.streaming

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

object StreamingFileSink {
  private val zoneId = ZoneId.of("Asia/Shanghai")
  private val batchFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val fileFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")

  def formatBatchTime(epochMillis: Long): String = {
    Instant.ofEpochMilli(epochMillis).atZone(zoneId).format(batchFormatter)
  }

  def batchFileName(prefix: String, epochMillis: Long): String = {
    val batchTime = Instant.ofEpochMilli(epochMillis).atZone(zoneId).format(fileFormatter)
    s"${prefix}_${batchTime}.txt"
  }

  def writeBatch(outputDir: String, fileName: String, rows: Seq[String]): Unit = {
    if (rows.nonEmpty) {
      val dir = Paths.get(outputDir)
      Files.createDirectories(dir)
      val file = dir.resolve(fileName)
      val content = (rows.mkString(System.lineSeparator()) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8)
      Files.write(
        file,
        content,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE
      )
    }
  }

  def marker(outputDir: String, fileName: String, line: String): Unit = {
    val dir: Path = Paths.get(outputDir)
    Files.createDirectories(dir)
    Files.write(
      dir.resolve(fileName),
      (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
  }
}
