package cn.ioceye.utils

import cn.ioceye.counter.mdtweek.MDTDay
import org.apache.spark.sql.Row

import scala.collection.mutable._

object MDTWeekFunction {
  def convertMdtByRow (iter: Iterator[Row]): Iterator[(String, MDTDay)] = {
    val appendHandler = new StringBuilder

    val mdtIter = iter.map(row => {
      val mdt = MDTDay.getInstance(row, appendHandler)
      val key = MDTDay.getKey(mdt.gridId20, mdt.earfcn, appendHandler)

      (key, mdt)
    })

    mdtIter
  }

  def reduceByKey (mdt: MDTDay, other: MDTDay): MDTDay = {
    MDTDay.mergeCounter(mdt, other)
  }

  def convertByGrid (iter: Iterator[(String, MDTDay)]): Iterator[(Long, MDTDay)] = {
    iter.map(tuple => {
      (tuple._2.gridId20, tuple._2)
    })
  }

  def convertToRow (iter: Iterator[(Long, scala.Iterable[MDTDay])], exportTime: DotTime): Iterator[Row] = {
    val gridMdtList = iter.toList

    val resRow = Set[Row]()
    for(gridMdt <- gridMdtList) {
      val mdtList = gridMdt._2.toList

      // max
      var maxAvgRsrp: Float = Float.MinValue
      var earfcnInMaxRsrp: Integer = null
      for(mdt <- mdtList) {
        val avgRsrp = mdt.sumRsrp / mdt.totalCount
        if (maxAvgRsrp < avgRsrp) {
          maxAvgRsrp = avgRsrp
          earfcnInMaxRsrp = mdt.earfcn
        }
      }

      for(mdt <- mdtList) {
        var mark: Integer = null
        if (earfcnInMaxRsrp == mdt.earfcn) {
          mark = 1
        }
        val r: Row = MDTDay.convertToRow(mdt, mark, exportTime)

        resRow.add(r)
      }
    }

    resRow.toIterator
  }
}
