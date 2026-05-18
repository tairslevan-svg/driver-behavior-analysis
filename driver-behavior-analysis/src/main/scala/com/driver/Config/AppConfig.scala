package com.driver.Config

case class AppConfig(
                      inputPath: String,
                      outputPath: String,
                      accelThreshold: Double = 3.0,
                      decelThreshold: Double = -3.0,
                      lateralThreshold: Double = 4.0
                    )

object AppConfig {
  def parse(args: Array[String]): AppConfig = {
    if (args.length < 2) {
      println("Usage: MainApp <inputPath> <outputPath> [accelThreshold] [decelThreshold] [lateralThreshold]")
      println("Example: MainApp input.csv output/ 2.5 -3.5 3.0")
      System.exit(1)
    }
    try {
      val input = args(0)
      val output = args(1)
      val accel = if (args.length >= 3) args(2).toDouble else 3.0
      val decel = if (args.length >= 4) args(3).toDouble else -3.0
      val lateral = if (args.length >= 5) args(4).toDouble else 4.0
      AppConfig(input, output, accel, decel, lateral)
    } catch {
      case e: NumberFormatException =>
        println("Error: Threshold parameters must be valid decimal numbers.")
        println("Usage: MainApp <inputPath> <outputPath> [accelThreshold] [decelThreshold] [lateralThreshold]")
        System.exit(1)
        // 补充抛出异常，解决Scala编译器的类型不匹配编译报错
        throw new IllegalArgumentException("Invalid threshold parameters")
    }
  }}