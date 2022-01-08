package cn.ioceye.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}

object SparkUtil {
  def getBuilder(): SparkSession.Builder = {
    val builder = SparkSession.builder()
    builder
  }

  def getSparkSession(sparkConf: SparkConf): SparkSession = {
    val builder = getBuilder()
    builder.config(sparkConf)
    builder.getOrCreate()
  }

  def setHiveSession(sparkSession: SparkSession): Unit = {
    sparkSession.sql("set hive.exec.compress.output=true")
    sparkSession.sql("set mapred.output.compression.type=BLOCK")
    sparkSession.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")

    sparkSession.sql("set hive.exec.compress.intermediate=true")
    sparkSession.sql("set hive.intermediate.compression.type=BLOCK")
    sparkSession.sql("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }
}