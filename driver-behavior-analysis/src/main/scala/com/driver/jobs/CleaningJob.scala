package jobs

import com.driver.transformation.DataCleaner
import com.driver.output.DataWriter
import org.apache.spark.sql.SparkSession
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
      // 本地测试时解开下面这行，集群提交时注释掉
      // .master("local[*]")
      .getOrCreate()

    try {
      // 1. 读取数据 + 类型转换（适配DataCleaner已有的方法）
      val raw = DataCleaner.cleanAndCastTypes(spark, sensorsPath)
      println("====原始数据描述统计====")
      raw.describe().show()

      // ==================== 核心修复：手动串清洗步骤，代替不存在的 clean() 方法 ====================
      // 因为DataCleaner里没有统一的clean方法，所以我们在Job里手动调用两个已有的清洗方法
      // 第一步：用均值填充温度空值
      val afterFillTemp = DataCleaner.fillMissingTemp(raw)
      // 第二步：过滤速度>200的异常数据
      val cleaned = DataCleaner.filterAbnormalSpeed(afterFillTemp)
      // ==========================================================================================

      println("====清洗后数据（前10行）====")
      cleaned.show(10, false)

      println(s"清洗后温度空值数量：${cleaned.filter(col("temp").isNull).count()}")
      println(s"清洗后速度>200的数量：${cleaned.filter(col("speed") > 200).count()}")

      DataWriter.writeCSV(cleaned, s"$outputPath/cleaned_sensors")
      DataWriter.writeParquet(cleaned, s"$outputPath/cleaned_sensors_parquet")

      println(s"清洗结果已写入$outputPath")
    } finally {
      spark.stop()
    }
  }
}