package cn.ioceye.core.sorted

import scala.collection.generic.Sorted

class SencondarySortKey(val first: Int, val second: Int) extends Ordered[SencondarySortKey] with Serializable {
  def compare(that: SencondarySortKey): Int = {
    val firstDis = first - that.first;
    if(firstDis != 0) {
      firstDis
    }
    else {
      second - that.second
    }
  }
}

object SencondarySortKey {
  def apply(first: Int, second: Int): SencondarySortKey = {
    new SencondarySortKey(first, second)
  }
}

