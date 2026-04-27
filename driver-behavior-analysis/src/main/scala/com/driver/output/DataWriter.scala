package com.driver.output

import org.apache.spark.sql.{DataFrame, SaveMode}

object DataWriter {
  def writeCSV(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(path)
  }

  def writeParquet(df: DataFrame, path: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }
}
