package cn.ioceye.utils

import cn.ioceye.counter.mdtweek.Nei
import scala.collection.mutable

object SortUtil {
  def sortMap(map: mutable.HashMap[String, Nei], helpList: java.util.LinkedList[Nei]): java.util.LinkedList[Nei] = {
    helpList.clear()
    for(next <- map.iterator) {

    }
    helpList
  }
}
