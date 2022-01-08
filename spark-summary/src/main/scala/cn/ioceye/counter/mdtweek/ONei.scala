package cn.ioceye.counter.mdtweek

import java.lang.{Float => JFloat}

import cn.ioceye.utils.CounterUtil.{add, convertBytesToFloat, convertToInt, multi}
import org.apache.spark.sql.Row

object ONei {
  def getInstance(row: Row, indexs: Int*): ONei = {
    val enbId      : Integer = convertToInt(row, indexs(0))
    val cellId     : Integer = convertToInt(row, indexs(1))
    val pci        : Integer = convertToInt(row, indexs(2))
    val avgRsrp    : JFloat  = convertBytesToFloat(row, indexs(3))
    val totalCount : Integer = convertToInt(row, indexs(4))
    val sumRsrp    : JFloat = multi(avgRsrp, totalCount)
    if (enbId == null && pci == null) {
      null
    }
    else {
      ONei(enbId, cellId, pci, sumRsrp, totalCount)
    }
  }

  def mergeCounter(nei: ONei, row: Row, indexs: Int*): ONei = {
    val avgRsrp    : JFloat  = convertBytesToFloat(row, indexs(3))
    val totalCount : Integer = convertToInt(row, indexs(4))
    val sumRsrp    : JFloat = multi(avgRsrp, totalCount)

    nei.totalCount = add(totalCount, nei.totalCount)
    nei.sumRsrp = add(sumRsrp, nei.sumRsrp)

    nei
  }

  def mergeCounter(src: ONei, other: ONei): ONei = {
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

case class ONei(var enbId: Integer,
               var cellId: Integer,
               var pci: Integer,
               var sumRsrp: JFloat,
               var totalCount: Integer) extends  Serializable {
  def getAvgRsrp(): Int = {
    (sumRsrp / totalCount * 100).toInt
  }
}
