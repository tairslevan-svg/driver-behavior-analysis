package com.driver.jobs

import com.driver.transformation.DataCleaner
import com.driver.output.DataWriter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CleaningJob {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: CleaningJob <sensorsPath> <outputPath>")
      System.exit(1)
    }
    val sensorsPath = args(0)
    val outputPath = args(1)
    val spark = SparkSession.builder()
      .appName("DataCleaning")
      // 本地测试解开这行，集群运行注释掉
      // .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取原始数据
      val raw = DataCleaner.readSensors(spark, sensorsPath)
      println("========== 清洗前原始数据 ==========")
      printStats(raw, "原始数据")

      // 2. 对原始数据用 IQR 方法识别速度异常值
      detectAndWriteOutliers(raw, outputPath, "original_outliers_iqr")

      // 3. 执行清洗（填充温度缺失值 + 剔除速度 > 200）
      val cleaned = DataCleaner.clean(raw)
      println("\n========== 清洗后数据 ==========")
      printStats(cleaned, "清洗后数据")

      // 4. 对清洗后数据再次用 IQR 方法识别
      detectAndWriteOutliers(cleaned, outputPath, "cleaned_outliers_iqr")

      // 5. 保存清洗后的完整数据
      DataWriter.writeCSV(cleaned, s"$outputPath/cleaned_sensors")
      DataWriter.writeParquet(cleaned, s"$outputPath/cleaned_sensors_parquet")
      println(s"\n清洗后的完整数据已写入 $outputPath/cleaned_sensors")
    } finally {
      spark.stop()
    }
  }

  /** 打印统计信息 */
  def printStats(df: DataFrame, name: String): Unit = {
    println(s"--- $name 统计 ---")
    println(s"总记录数: ${df.count()}")
    println(s"温度缺失值数量: ${df.filter(col("temp").isNull).count()}")
    println(s"速度 > 200 的数量: ${df.filter(col("speed") > 200).count()}")
    println("描述性统计:")
    df.describe().show()
  }

  /** IQR异常检测 */
  def detectAndWriteOutliers(df: DataFrame, baseOutputPath: String, subPath: String): Unit = {
    val quantiles = df.stat.approxQuantile("speed", Array(0.25, 0.75), 0.0)
    val q1 = quantiles(0)
    val q3 = quantiles(1)
    val iqr = q3 - q1
    val lowerBound = q1 - 1.5 * iqr
    val upperBound = q3 + 1.5 * iqr
    println(s"\n--- IQR 异常检测 ($subPath) ---")
    println(s"Q1 = $q1, Q3 = $q3, IQR = $iqr")
    println(s"IQR 正常范围: [$lowerBound, $upperBound]")
    val outliers = df.filter(col("speed") < lowerBound || col("speed") > upperBound)
    val outlierCount = outliers.count()
    println(s"使用 IQR 方法识别的异常值数量: $outlierCount")
    if (outlierCount > 0) {
      println("异常值样本（前10条）:")
      outliers.show(10, truncate = false)
      DataWriter.writeCSV(outliers, s"$baseOutputPath/$subPath")
      DataWriter.writeParquet(outliers, s"$baseOutputPath/${subPath}_parquet")
      println(s"异常值已写入 $baseOutputPath/$subPath")
    } else {
      println("未发现异常值。")
    }
  }
}