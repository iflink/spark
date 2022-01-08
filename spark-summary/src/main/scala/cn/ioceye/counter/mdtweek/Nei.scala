package cn.ioceye.counter.mdtweek

import cn.ioceye.utils.CounterUtil.{convertBytesToFloat, convertToInt, multi, add}
import org.apache.spark.sql.Row
import java.lang.{Float => JFloat}

object Nei {
  def getInstance(row: Row, indexes: Int*): Nei = {
    val enbId      : Integer = convertToInt(row, indexes(0))
    val cellId     : Integer = convertToInt(row, indexes(1))
    val pci        : Integer = convertToInt(row, indexes(2))
    val avgRsrp    : JFloat  = convertBytesToFloat(row, indexes(3))
    val totalCount : Integer = convertToInt(row, indexes(4))
    val sumRsrp    : JFloat = multi(avgRsrp, totalCount)
    if (enbId == null && pci == null) {
      null
    }
    else {
      Nei(enbId, cellId, pci, sumRsrp, totalCount)
    }
  }

  def mergeCounter(src: Nei, other: Nei): Nei = {
    src.totalCount = add(other.totalCount, src.totalCount)
    src.sumRsrp = add(other.sumRsrp, src.sumRsrp)

    src
  }

  def getKey(row: Row, appendHandler: StringBuilder, indexs: Int*): String = {
    val enbId = convertToInt(row, indexs(0))
    val cellId = convertToInt(row, indexs(1))
    val pci = convertToInt(row, indexs(2))

    appendHandler.clear()
    if (enbId != null) {
      appendHandler.append(enbId).append("-").append(cellId)
      appendHandler.toString
    }
    else if (pci != null) {
      appendHandler.append(pci)
      appendHandler.toString
    }
    else {
      null
    }
  }
}

case class Nei(var enbId: Integer,
               var cellId: Integer,
               var pci: Integer,
               var sumRsrp: JFloat,
               var totalCount: Integer) extends  Serializable {
  var avgRsrp: Float = _
  def getAvgRsrp(): Int = {
    avgRsrp = sumRsrp / totalCount * 100
    avgRsrp.toInt
  }
}
