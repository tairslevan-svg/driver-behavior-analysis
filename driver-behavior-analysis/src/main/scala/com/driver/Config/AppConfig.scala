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
    // 简单的参数解析，期望两个参数：输入路径 输出路径
    if (args.length < 2) {
      println("Usage: MainApp <inputPath> <outputPath>")
      System.exit(1)
    }
    AppConfig(args(0), args(1))
  }
}