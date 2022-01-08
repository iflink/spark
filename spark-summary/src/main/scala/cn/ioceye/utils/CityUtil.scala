package cn.ioceye.utils

import scala.collection.mutable

object CityUtil {
  private[this] val cityMap = new mutable.HashMap[Int, String]()

  {
    cityMap += (25 -> "南京")
    cityMap += (512 -> "苏州")
    cityMap += (510 -> "无锡")
    cityMap += (513 -> "南通")
    cityMap += (519 -> "常州")
    cityMap += (511 -> "镇江")
    cityMap += (516 -> "徐州")
    cityMap += (514 -> "扬州")
    cityMap += (523 -> "泰州")
    cityMap += (515 -> "盐城")
    cityMap += (518 -> "连云港")
    cityMap += (527 -> "宿迁")
    cityMap += (517 -> "淮安")
  }

  def getCityName(cityId: Int): String = {
    cityMap.getOrElse(cityId, null)
  }
}
