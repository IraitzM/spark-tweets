package org.deusto

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppConfig {

  def getSession(appName: String): SparkSession = {
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "true")
    sparkConfig.set("spark.shuffle.compress", "true")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.io.compression.codec", "lzf")

    SparkSession.builder
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate
  }

  // Local session for testing purposes
  def getLocalSession(): SparkSession = {
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    sparkConfig.set("spark.io.compression.codec", "lzf")

    SparkSession.builder().appName("SparkTest").master("local").getOrCreate()

  }
}
