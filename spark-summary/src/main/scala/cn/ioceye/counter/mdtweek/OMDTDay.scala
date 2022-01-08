package cn.ioceye.counter.mdtweek

import java.lang.{Float => JFloat}

import cn.ioceye.utils.CounterUtil._
import org.apache.spark.sql.Row

import scala.collection.mutable.HashMap

object OMDTDay {
  def getInstance(row: Row, appendHandler: StringBuilder = new StringBuilder): OMDTDay = {
    mergeCounter(null, row, appendHandler)
  }

  def mergeCounter(mdt: OMDTDay, row: Row, appendHandler: StringBuilder = new StringBuilder): OMDTDay = {
    var _mdt: OMDTDay = mdt
    if (mdt == null) {
      _mdt = OMDTDay()

      _mdt.gridId20 = row.getLong(0)
      _mdt.earfcn = row.getFloat(1).toInt
      _mdt.cityId = row.getInt(8)
    }

    _mdt.rsrpNum105 += row.getInt(2)
    _mdt.rsrpNum110 += row.getInt(3)
    _mdt.rsrpNum114 += row.getInt(4)

    val totalCount: Int = row.getInt(7)
    _mdt.totalCount += totalCount
    _mdt.sumRsrp = getString(row,5).toFloat * totalCount
    _mdt.sumRsrq = getString(row,6).toFloat * totalCount


    // nei map
    var neiMap:HashMap[String, ONei] = _mdt.neiMap
    import util.control.Breaks._
    appendHandler.clear()
    breakable(
      for(index <- 0 to 15 by 5) {
        val enbIdIndex  = 10 + index
        val cellIdIndex = 11 + index
        val pciIndex    = 12 + index
        val avgRsrpIndex= 13 + index
        val totalIndex  = 14 + index

        val neiKey = Nei.getKey(row, appendHandler, enbIdIndex, cellIdIndex, pciIndex)
        var nei: ONei = neiMap.getOrElse(neiKey, null)
        if (nei == null) {
          nei = ONei.getInstance(row, enbIdIndex, cellIdIndex, pciIndex, avgRsrpIndex, totalIndex)
          if (nei == null) {
            break()
          }
          neiMap += (neiKey -> nei)
        }
        else {
          ONei.mergeCounter(nei, row,  enbIdIndex, cellIdIndex, pciIndex, avgRsrpIndex, totalIndex)
        }
      }
    )

    _mdt.num1 += row.getInt(30)
    _mdt.num2 += row.getInt(31)
    _mdt.num3 += row.getInt(32)

    _mdt.aboveNum3 += row.getInt(33)

    val phrCount = row.getInt(36)
    _mdt.phrCount += phrCount
    _mdt.sumPhr = add(multi(convertBytesToFloat(row, 34), phrCount), _mdt.sumPhr)
    _mdt.sumPhrAboveZeroPatio = add(multi(convertBytesToFloat(row, 35), phrCount), _mdt.sumPhrAboveZeroPatio)

    val sinrCount = row.getInt(38)
    _mdt.sinrCount += sinrCount
    _mdt.sumSinr = add(multi(convertBytesToFloat(row, 37), sinrCount), _mdt.sumSinr)

    var totalCqi: Integer = null
    val sumCqi = multi(convertBytesToFloat(row, 39), totalCqi)
    if (sumCqi != null) {
      _mdt.totalCqi = add(totalCqi, _mdt.totalCqi)
      _mdt.sumCqi = add(sumCqi,  _mdt.sumCqi)
    }

    _mdt.sumLtescplrulqci1 = add(multi(convertBytesToFloat(row, 40), totalCount), _mdt.sumLtescplrulqci1)
    _mdt.totalLtescplrulqci1 = add(_mdt.totalLtescplrulqci1, totalCount)

    _mdt.sumLtescplrulqci5 = add(multi(convertBytesToFloat(row, 41), totalCount), _mdt.sumLtescplrulqci1)
    _mdt.totalLtescplrulqci5 = add(_mdt.totalLtescplrulqci1, totalCount)

    _mdt.sumLtescplrulqci9 = add(multi(convertBytesToFloat(row, 42), totalCount), _mdt.sumLtescplrulqci1)
    _mdt.totalLtescplrulqci9 = add(_mdt.totalLtescplrulqci1, totalCount)

    _mdt.sumLtescplrdlqci1 = add(multi(convertBytesToFloat(row, 43), totalCount), _mdt.sumLtescplrulqci1)
    _mdt.totalLtescplrdlqci1 = add(_mdt.totalLtescplrulqci1, totalCount)

    _mdt.sumLtescplrdlqci5 = add(multi(convertBytesToFloat(row, 44), totalCount), _mdt.sumLtescplrulqci1)
    _mdt.totalLtescplrdlqci5 = add(_mdt.totalLtescplrulqci1, totalCount)

    _mdt.sumLtescplrdlqci9 = add(multi(convertBytesToFloat(row, 45), totalCount), _mdt.sumLtescplrulqci1)
    _mdt.totalLtescplrdlqci9 = add(_mdt.totalLtescplrulqci1, totalCount)

    val rsrpGt105Count = convertToInt(row, 46)
    _mdt.rsrpGt105Count = add(rsrpGt105Count, _mdt.rsrpGt105Count)
    _mdt.sumRsrpGt105 = add(multi(convertBytesToFloat(row, 47), rsrpGt105Count), _mdt.sumRsrpGt105)

    val rsrpLt105Count = convertToInt(row, 48)
    _mdt.rsrpLt105Count = add(rsrpLt105Count, _mdt.rsrpLt105Count)
    _mdt.sumRsrpLt105 = add(multi(convertBytesToFloat(row, 49), rsrpLt105Count), _mdt.sumRsrpLt105)

    _mdt.agpsusers += row.getInt(50)
    val mrCqiCount = convertToInt(row, 61)
    _mdt.mrCqiCount = add(mrCqiCount, _mdt.mrCqiCount)
    _mdt.sumPctCqiLte7 = add(multi(convertBytesToFloat(row, 53), mrCqiCount), _mdt.sumPctCqiLte7)

    _mdt
  }

  def getKey0(row: Row, appendHandler: StringBuilder = new StringBuilder): String = {
    val gridId20 = row.getLong(0)
    val earfcn = row.getFloat(1).toInt

    appendHandler.clear()
    appendHandler.append(gridId20).append("-").append(earfcn)
    appendHandler.toString()
  }

  def getKey(gridId20: Long, earfcn: Int, appendHandler: StringBuilder = new StringBuilder): String = {
    appendHandler.clear()
    appendHandler.append(gridId20).append("-").append(earfcn)
    appendHandler.toString()
  }
}

case class OMDTDay (
                  var gridId20: Long = 0,
                  var earfcn: Int = 0,
                  var cityId: Int = 0,
                  var rsrpNum105: Int = 0,
                  var rsrpNum110: Int = 0,
                  var rsrpNum114: Int = 0,
                  var sumRsrp: Float  = 0.0f,
                  var sumRsrq: Float  = 0.0f,
                  var totalCount: Int = 0,
                  var neiMap: HashMap[String, ONei] = new HashMap[String, ONei],
                  var num1: Int = 0,
                  var num2: Int = 0,
                  var num3: Int = 0,
                  var aboveNum3: Int = 0,
                  var sumPhr: JFloat = null,
                  var sumPhrAboveZeroPatio: JFloat = null,
                  var phrCount: Int = 0,
                  var sumSinr: Float = 0.0f,
                  var sinrCount: Int = 0,
                  var sumCqi: JFloat = null,
                  var totalCqi: Integer = null,
                  var sumLtescplrulqci1: JFloat = null,
                  var totalLtescplrulqci1: Integer = null,
                  var sumLtescplrulqci5: JFloat = null,
                  var totalLtescplrulqci5: Integer = null,
                  var sumLtescplrulqci9: JFloat = null,
                  var totalLtescplrulqci9: Integer = null,
                  var sumLtescplrdlqci1: JFloat = null,
                  var totalLtescplrdlqci1: Integer = null,
                  var sumLtescplrdlqci5: JFloat = null,
                  var totalLtescplrdlqci5: Integer = null,
                  var sumLtescplrdlqci9: JFloat = null,
                  var totalLtescplrdlqci9: Integer = null,
                  var rsrpGt105Count: Integer = null,
                  var sumRsrpGt105:  JFloat = null,
                  var rsrpLt105Count: Integer = null,
                  var sumRsrpLt105: JFloat = null,
                  var agpsusers: Int = 0,
                  var mrCqiCount: Integer = null,
                  var sumPctCqiLte7: JFloat = null
                ) extends Serializable