package cn.ioceye.utils

import java.util.Date

object UWayImpalaHome {
  /**
   * 集群内提交，不需要NameSpace
   * @type String
   */
  val defaultNS: String = "hdfs://snn:8020"
  val defaultDB: String = "/user/impala/lte_hd"

  def getAbsolutePath(table: String, year: Int, month: Int, day: Int,
                      ns: String = defaultNS, db: String = defaultDB,
                      appendHandler: StringBuilder = new StringBuilder): String = {
    appendHandler.clear()

    appendHandler.append(ns).append(db)
      .append("/").append(table)
      .append("/year=").append(year)
      .append("/month=").append(month)
      .append("/day=").append(day)

    appendHandler.toString()
  }

  def getWeekAbsolutePath(date: Date, table: String, ns: String = defaultNS): Array[String] = {
    val appendHandler: StringBuilder = new StringBuilder
    val dotTime: DotTime = TimeUtil.currentWeekOfMonday(date)

    val weekAbsolutePaths: Array[String] = new Array[String](7)
    for(incr <- 0 until 7) {
      weekAbsolutePaths(incr) = getAbsolutePath(table, dotTime.year, dotTime.month, dotTime.day, ns, appendHandler=appendHandler)
      dotTime.nextDay()
    }
    weekAbsolutePaths
  }


  def getWeekAbsolutePath0(date: Date, table: String): Array[String] = {
    val appendHandler: StringBuilder = new StringBuilder
    val dotTime: DotTime = TimeUtil.currentWeekOfMonday(date)

    val weekAbsolutePaths: Array[String] = new Array[String](7)
    for(incr <- 0 until 7) {
      weekAbsolutePaths(incr) = getAbsolutePath(table, dotTime.year, dotTime.month, dotTime.day, "", appendHandler=appendHandler)
      dotTime.nextDay()
    }
    weekAbsolutePaths
  }
}
