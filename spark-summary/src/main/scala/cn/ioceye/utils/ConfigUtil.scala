package cn.ioceye.utils

import java.util.ResourceBundle

import org.apache.spark.SparkConf

object ConfigUtil {
  private[this] var resource: ResourceBundle = _
  // initial
  initialLoading

  def initialLoading(): Unit = {
    resource =  ResourceBundle.getBundle("config")
  }

  def setConfig(sparkConfig: SparkConf): Unit = {
    val keyEnum = resource.getKeys
    while(keyEnum.hasMoreElements) {
      val key: String = keyEnum.nextElement()
      val value: String = resource.getString(key)

      sparkConfig.set(key, value)
    }
  }

  def get(key: String): String = {
    resource.getString(key)
  }
}
