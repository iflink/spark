package cn.ioceye.counter.mdtweek

import java.lang.{Float => JFloat}

import cn.ioceye.utils.CounterUtil._
import cn.ioceye.utils.{CityUtil, DotTime}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{DataTypes, StructType}

object MDTDay {
  def getInstance(row: Row, appendHandler: StringBuilder): MDTDay = {
    val mdt = MDTDay()

    mdt.gridId20 = row.getLong(0)
    mdt.earfcn = row.getFloat(1).toInt
    mdt.cityId = row.getInt(8)

    mdt.rsrpNum105 = row.getInt(2)
    mdt.rsrpNum110 = row.getInt(3)
    mdt.rsrpNum114 = row.getInt(4)

    val totalCount: Int = row.getInt(7)
    mdt.totalCount = totalCount
    mdt.sumRsrp = multi(getStringToFloat(row,5), totalCount)
    mdt.sumRsrq = multi(getStringToFloat(row,6), totalCount)

    // nei map
    var neiMap: scala.collection.mutable.HashMap[String, Nei] = mdt.neiMap
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
        val  nei = Nei.getInstance(row, enbIdIndex, cellIdIndex, pciIndex, avgRsrpIndex, totalIndex)
        if (nei == null) {
          break()
        }
        neiMap += (neiKey -> nei)
      }
    )
    mdt.num1 = row.getInt(30)
    mdt.num2 = row.getInt(31)
    mdt.num3 = row.getInt(32)

    mdt.aboveNum3 = row.getInt(33)

    val phrCount = row.getInt(36)
    mdt.phrCount = phrCount
    mdt.sumPhr = multi(convertBytesToFloat(row, 34), phrCount)
    mdt.sumPhrAboveZeroPatio = multi(convertBytesToFloat(row, 35), phrCount)

    val sinrCount = row.getInt(38)
    mdt.sinrCount = sinrCount
    mdt.sumSinr = multi(convertBytesToFloat(row, 37), sinrCount)

    val sumCqi = multi(convertBytesToFloat(row, 39), totalCount)
    if (sumCqi != null) {
      mdt.totalCqi = totalCount
      mdt.sumCqi = sumCqi
    }

    mdt.sumLtescplrulqci1 = multi(convertBytesToFloat(row, 40), totalCount)
    mdt.totalLtescplrulqci1 = getIf(mdt.sumLtescplrulqci1, totalCount)

    mdt.sumLtescplrulqci5 = multi(convertBytesToFloat(row, 41), totalCount)
    mdt.totalLtescplrulqci5 = getIf(mdt.sumLtescplrulqci5, totalCount)

    mdt.sumLtescplrulqci9 = multi(convertBytesToFloat(row, 42), totalCount)
    mdt.totalLtescplrulqci9 = getIf(mdt.sumLtescplrulqci9, totalCount)

    mdt.sumLtescplrdlqci1 = multi(convertBytesToFloat(row, 43), totalCount)
    mdt.totalLtescplrdlqci1 = getIf(mdt.sumLtescplrdlqci1, totalCount)

    mdt.sumLtescplrdlqci5 = multi(convertBytesToFloat(row, 44), totalCount)
    mdt.totalLtescplrdlqci5 = getIf(mdt.sumLtescplrdlqci5, totalCount)

    mdt.sumLtescplrdlqci9 = multi(convertBytesToFloat(row, 45), totalCount)
    mdt.totalLtescplrdlqci9 = getIf(mdt.sumLtescplrdlqci9, totalCount)

    val rsrpGt105Count = convertToInt(row, 46)
    mdt.rsrpGt105Count = rsrpGt105Count
    mdt.sumRsrpGt105 = multi(convertBytesToFloat(row, 47), rsrpGt105Count)

    val rsrpLt105Count = convertToInt(row, 48)
    mdt.rsrpLt105Count = rsrpLt105Count
    mdt.sumRsrpLt105 = multi(convertBytesToFloat(row, 49), rsrpLt105Count)

    mdt.agpsusers = row.getInt(50)
    mdt.sumRip = multi(convertBytesToFloat(row, 52), mdt.totalCount)

    mdt.mrCqiCount = convertToInt(row, 61)
    mdt.sumPctCqiLte7 = multi(convertBytesToFloat(row, 53), mdt.mrCqiCount)

    mdt
  }

  def mergeCounter(src: MDTDay, other: MDTDay): MDTDay = {
    src.rsrpNum105 += other.rsrpNum105
    src.rsrpNum110 += other.rsrpNum110
    src.rsrpNum114 += other.rsrpNum114

    src.totalCount += other.totalCount
    src.sumRsrp = add(src.sumRsrp, other.sumRsrp)
    src.sumRsrq = add(src.sumRsrq, other.sumRsrq)

    // nei map
    val otherNeiMapIter = other.neiMap.toIterator
    var srcNeiMap: scala.collection.mutable.HashMap[String, Nei] = src.neiMap
    while(otherNeiMapIter.hasNext) {
      val nextEntry = otherNeiMapIter.next()
      val nei = srcNeiMap.getOrElse(nextEntry._1, null)
      if (nei == null) {
        srcNeiMap += nextEntry
      }
      else {
        Nei.mergeCounter(nei, nextEntry._2)
      }
    }

    src.num1 += other.num1
    src.num2 += other.num2
    src.num3 += other.num3

    src.aboveNum3 += other.aboveNum3

    src.phrCount += other.phrCount
    src.sumPhr = add(src.sumPhr, other.sumPhr)
    src.sumPhrAboveZeroPatio = add(src.sumPhrAboveZeroPatio, other.sumPhrAboveZeroPatio)

    src.sinrCount += other.sinrCount
    src.sumSinr = add(src.sumSinr, other.sumSinr)

    src.totalCqi = add(src.totalCqi, other.totalCqi)
    src.sumCqi = add(src.sumCqi, other.sumCqi)

    src.sumLtescplrulqci1 = add(src.sumLtescplrulqci1, other.sumLtescplrulqci1)
    src.totalLtescplrulqci1 = add(src.totalLtescplrulqci1, other.totalLtescplrulqci1)

    src.sumLtescplrulqci5 = add(src.sumLtescplrulqci5, other.sumLtescplrulqci5)
    src.totalLtescplrulqci5 = add(src.totalLtescplrulqci5, other.totalLtescplrulqci5)

    src.sumLtescplrulqci9 = add(src.sumLtescplrulqci9, other.sumLtescplrulqci9)
    src.totalLtescplrulqci9 = add(src.totalLtescplrulqci9, other.totalLtescplrulqci9)

    src.sumLtescplrdlqci1 = add(src.sumLtescplrdlqci1, other.sumLtescplrdlqci1)
    src.totalLtescplrdlqci1 = add(src.totalLtescplrdlqci1, other.totalLtescplrdlqci1)

    src.sumLtescplrdlqci5 = add(src.sumLtescplrdlqci5, other.sumLtescplrulqci1)
    src.totalLtescplrdlqci5 = add(src.totalLtescplrdlqci5, other.totalLtescplrdlqci5)

    src.sumLtescplrdlqci9 = add(src.sumLtescplrdlqci9, other.sumLtescplrdlqci9)
    src.totalLtescplrdlqci9 = add(src.totalLtescplrdlqci9, other.totalLtescplrdlqci9)

    src.rsrpGt105Count = add(src.rsrpGt105Count, other.rsrpGt105Count)
    src.sumRsrpGt105 = add(src.sumRsrpGt105, other.sumRsrpGt105)

    src.rsrpLt105Count = add(src.rsrpLt105Count, other.rsrpLt105Count)
    src.sumRsrpLt105 = add(src.sumRsrpLt105, other.sumRsrpLt105)

    src.agpsusers += other.agpsusers
    src.sumRip = add(src.sumRip, other.sumRip)
    src.mrCqiCount = add(src.mrCqiCount, other.mrCqiCount)
    src.sumPctCqiLte7 = add(src.sumPctCqiLte7, other.sumPctCqiLte7)

    src
  }

  def getKey(gridId20: Long, earfcn: Int, appendHandler: StringBuilder = new StringBuilder): String = {
    appendHandler.clear()
    appendHandler.append(gridId20).append("-").append(earfcn)
    appendHandler.toString()
  }

  def convertToRow(mdt: MDTDay, mark: Integer, exportTime: DotTime): Row = {
    var counters = new Array[Any](62)
    counters(0) = mdt.gridId20
    counters(1) = mdt.earfcn
    counters(2) = mdt.rsrpNum105
    counters(3) = mdt.rsrpNum110
    counters(4) = mdt.rsrpNum114
    if (mdt.sumRsrq != null) {
      counters(5) = "%.2f".format(mdt.sumRsrp / mdt.totalCount)
    }
    if (mdt.sumRsrq != null) {
      counters(6) = "%.2f".format(mdt.sumRsrq / mdt.totalCount)
    }
    counters(7) = mdt.totalCount
    counters(8) = mdt.cityId
    counters(9) = CityUtil.getCityName(mdt.cityId)

    // =================== nei =========================
    val neis = mdt.neiMap.toSeq
    val descNeiSeq = neis.sortWith((o1, o2) => {
      o1._2.getAvgRsrp() > o2._2.getAvgRsrp()
    })
    // one nei
    if (mdt.neiMap.size == 1) {
      val nei = mdt.neiMap.toList.head
      nei._2.getAvgRsrp()
    }
    val avgRsrps = new Array[JFloat](4)
    var neiCount = 0
    for(neiEntry <- descNeiSeq if neiCount <= 3) {
      val nei = neiEntry._2
      val incr = neiCount * 6
      val avgRsrp = nei.avgRsrp / 100

      counters(10 + incr) = nei.enbId
      counters(11 + incr) = nei.cellId
      counters(12 + incr) = nei.pci
      counters(13 + incr) = "%.2f".format(avgRsrp)
      counters(14 + incr) = nei.totalCount

      avgRsrps(neiCount) = avgRsrp

      neiCount += 1
    }
    // avg_rsrp
    if (avgRsrps(0) != null) {
      // avg_rsrp2
      if (avgRsrps(1) != null) {
        counters(15) = "%.2f".format(avgRsrps(0) - avgRsrps(1))
      }
      else {
        counters(15) = "63"
      }
    }
    if (avgRsrps(1) != null) {
      counters(21) = "%.2f".format(avgRsrps(1) - avgRsrps(0))
    }
    if (avgRsrps(2) != null) {
      counters(27) = "%.2f".format(avgRsrps(2) - avgRsrps(0))
    }
    if (avgRsrps(3) != null) {
      counters(33) = "%.2f".format(avgRsrps(3) - avgRsrps(0))
    }

    // ================== other counters =======================
    counters(34) = mdt.num1
    counters(35) = mdt.num2
    counters(36) = mdt.num3
    counters(37) = mdt.aboveNum3
    if (mdt.phrCount > 0) {
      if (mdt.sumPhr != null) {
        counters(38) = "%.2f".format(mdt.sumPhr / mdt.phrCount)
      }
      if (mdt.sumPhrAboveZeroPatio != null) {
        counters(39) = "%.2f".format(mdt.sumPhrAboveZeroPatio / mdt.phrCount)
      }
    }
    counters(40) = mdt.phrCount
    if (mdt.sumSinr != null &&mdt.sinrCount > 0) {
      counters(41) = "%.2f".format(mdt.sumSinr / mdt.sinrCount)
    }
    counters(42) = mdt.sinrCount
    if (mdt.totalCqi != null && mdt.totalCqi > 0) {
      counters(43) = "%.2f".format(mdt.sumCqi / mdt.totalCqi)
    }
    if (mdt.sumLtescplrulqci1 != null && mdt.totalLtescplrulqci1 != null && mdt.totalLtescplrulqci1 > 0) {
      counters(44) = "%.2f".format(mdt.sumLtescplrulqci1 / mdt.totalLtescplrulqci1)
    }
    if (mdt.sumLtescplrulqci5 != null && mdt.totalLtescplrulqci5 != null && mdt.totalLtescplrulqci5 > 0) {
      counters(45) = "%.2f".format(mdt.sumLtescplrulqci5 / mdt.totalLtescplrulqci5)
    }
    if (mdt.sumLtescplrulqci9 != null && mdt.totalLtescplrulqci9 != null && mdt.totalLtescplrulqci9 > 0) {
      counters(46) = "%.2f".format(mdt.sumLtescplrulqci9 / mdt.totalLtescplrulqci9)
    }
    if (mdt.sumLtescplrdlqci1 != null && mdt.totalLtescplrdlqci1 != null && mdt.totalLtescplrdlqci1 > 0) {
      counters(47) = "%.2f".format(mdt.sumLtescplrdlqci1 / mdt.totalLtescplrdlqci1)
    }
    if (mdt.sumLtescplrdlqci5 != null && mdt.totalLtescplrdlqci5 != null && mdt.totalLtescplrdlqci5 > 0) {
      counters(48) = "%.2f".format(mdt.sumLtescplrdlqci5 / mdt.totalLtescplrdlqci5)
    }
    if (mdt.sumLtescplrdlqci9 != null && mdt.totalLtescplrdlqci9 != null && mdt.totalLtescplrdlqci9 > 0) {
      counters(49) = "%.2f".format(mdt.sumLtescplrdlqci9 / mdt.totalLtescplrdlqci9)
    }
    if (mdt.rsrpGt105Count != null) {
      counters(50) = mdt.rsrpGt105Count
      if (mdt.rsrpGt105Count > 0 && mdt.sumRsrpGt105 != null) {
        counters(51) = "%.2f".format(mdt.sumRsrpGt105 / mdt.rsrpGt105Count)
      }
    }
    if (mdt.rsrpLt105Count != null) {
      counters(52) = mdt.rsrpLt105Count
      if (mdt.rsrpLt105Count > 0 && mdt.sumRsrpLt105 != null) {
        counters(53) = "%.2f".format(mdt.sumRsrpLt105 / mdt.rsrpLt105Count)
      }
    }
    counters(54) = mdt.agpsusers
    counters(55) = mark
    if (mdt.sumPctCqiLte7 == null || mdt.mrCqiCount == null || mdt.mrCqiCount == 0) {
      counters(57) = "0"
    }
    else {
        counters(57) = "%.2f".format(mdt.sumPctCqiLte7 / mdt.mrCqiCount)
    }
    counters(58) = mdt.mrCqiCount

    counters(59) = exportTime.year
    counters(60) = exportTime.month
    counters(61) = exportTime.day

    new GenericRow(counters)
  }

  def getSchema(): StructType = {
    val fieldTypes = List(
      DataTypes.createStructField("grid_id_20", DataTypes.LongType, true),
      DataTypes.createStructField("earfcn", DataTypes.IntegerType, true),
      DataTypes.createStructField("rsrp_num_105", DataTypes.IntegerType, true),
      DataTypes.createStructField("rsrp_num_110", DataTypes.IntegerType, true),
      DataTypes.createStructField("rsrp_num_114", DataTypes.IntegerType, true),
      // 5
      DataTypes.createStructField("avg_rsrp", DataTypes.StringType, true),
      DataTypes.createStructField("avg_rsrq", DataTypes.StringType, true),
      DataTypes.createStructField("totalcount", DataTypes.IntegerType, true),
      DataTypes.createStructField("city_id", DataTypes.IntegerType, true),
      DataTypes.createStructField("city_name", DataTypes.StringType, true),

      // 10
      DataTypes.createStructField("enb_id1", DataTypes.IntegerType, true),
      DataTypes.createStructField("cell_id1", DataTypes.IntegerType, true),
      DataTypes.createStructField("pci1", DataTypes.IntegerType, true),
      DataTypes.createStructField("avg_rsrp1", DataTypes.StringType, true),
      DataTypes.createStructField("totalcount1", DataTypes.IntegerType, true),
      DataTypes.createStructField("difbest1", DataTypes.StringType, true),
      // 16
      DataTypes.createStructField("enb_id2", DataTypes.IntegerType, true),
      DataTypes.createStructField("cell_id2", DataTypes.IntegerType, true),
      DataTypes.createStructField("pci2", DataTypes.IntegerType, true),
      DataTypes.createStructField("avg_rsrp2", DataTypes.StringType, true),
      DataTypes.createStructField("totalcount2", DataTypes.IntegerType, true),
      DataTypes.createStructField("difbest2", DataTypes.StringType, true),
      // 22
      DataTypes.createStructField("enb_id3", DataTypes.IntegerType, true),
      DataTypes.createStructField("cell_id3", DataTypes.IntegerType, true),
      DataTypes.createStructField("pci3", DataTypes.IntegerType, true),
      DataTypes.createStructField("avg_rsrp3", DataTypes.StringType, true),
      DataTypes.createStructField("totalcount3", DataTypes.IntegerType, true),
      DataTypes.createStructField("difbest3", DataTypes.StringType, true),
      // 28
      DataTypes.createStructField("enb_id4", DataTypes.IntegerType, true),
      DataTypes.createStructField("cell_id4", DataTypes.IntegerType, true),
      DataTypes.createStructField("pci4", DataTypes.IntegerType, true),
      DataTypes.createStructField("avg_rsrp4", DataTypes.StringType, true),
      DataTypes.createStructField("totalcount4", DataTypes.IntegerType, true),
      DataTypes.createStructField("difbest4", DataTypes.StringType, true),

      // 34
      DataTypes.createStructField("num1", DataTypes.IntegerType, true),
      DataTypes.createStructField("num2", DataTypes.IntegerType, true),
      DataTypes.createStructField("num3", DataTypes.IntegerType, true),
      DataTypes.createStructField("abovenum3", DataTypes.IntegerType, true),
      // 38
      DataTypes.createStructField("avg_phr", DataTypes.StringType, true),
      DataTypes.createStructField("phr_above_zero_ratio", DataTypes.StringType, true),
      // 40
      DataTypes.createStructField("phrcount", DataTypes.IntegerType, true),
      DataTypes.createStructField("avg_sinr", DataTypes.StringType, true),
      // 42
      DataTypes.createStructField("sinrcount", DataTypes.IntegerType, true),
      DataTypes.createStructField("avg_cqi", DataTypes.StringType, true),
      // 44
      DataTypes.createStructField("avg_ltescplrulqci1", DataTypes.StringType, true),
      DataTypes.createStructField("avg_ltescplrulqci5", DataTypes.StringType, true),
      DataTypes.createStructField("avg_ltescplrulqci9", DataTypes.StringType, true),
      DataTypes.createStructField("avg_ltescplrdlqci1", DataTypes.StringType, true),
      DataTypes.createStructField("avg_ltescplrdlqci5", DataTypes.StringType, true),
      DataTypes.createStructField("avg_ltescplrdlqci9", DataTypes.StringType, true),
      // 50
      DataTypes.createStructField("rsrp_gt_105_count", DataTypes.IntegerType, true),
      DataTypes.createStructField("avr_rsrp_gt_105", DataTypes.StringType, true),
      DataTypes.createStructField("rsrp_lt_105_count", DataTypes.IntegerType, true),
      DataTypes.createStructField("avr_rsrp_lt_105", DataTypes.StringType, true),
      DataTypes.createStructField("agpsusers", DataTypes.IntegerType, true),
      DataTypes.createStructField("mark", DataTypes.IntegerType, true),

      DataTypes.createStructField("avg_rip", DataTypes.StringType, true),
      DataTypes.createStructField("pci_cqi_gte7", DataTypes.StringType, true),
      DataTypes.createStructField("mr_cqicount", DataTypes.IntegerType, true),

      DataTypes.createStructField("year", DataTypes.IntegerType, true),
      DataTypes.createStructField("month", DataTypes.IntegerType, true),
      DataTypes.createStructField("day", DataTypes.IntegerType, true)
    )

    StructType(fieldTypes)
  }
}

case class MDTDay (
                  var gridId20: Long = 0,
                  var earfcn: Int = 0,
                  var cityId: Int = 0,
                  var rsrpNum105: Int = 0,
                  var rsrpNum110: Int = 0,
                  var rsrpNum114: Int = 0,
                  var sumRsrp: JFloat  = 0.0f,
                  var sumRsrq: JFloat  = 0.0f,
                  var totalCount: Int = 0,
                  var neiMap: scala.collection.mutable.HashMap[String, Nei] = new scala.collection.mutable.HashMap[String, Nei],
                  var num1: Int = 0,
                  var num2: Int = 0,
                  var num3: Int = 0,
                  var aboveNum3: Int = 0,
                  var sumPhr: JFloat = null,
                  var sumPhrAboveZeroPatio: JFloat = null,
                  var phrCount: Int = 0,
                  var sumSinr: JFloat = 0.0f,
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
                  var sumRip: JFloat = null,
                  var mrCqiCount: Integer = null,
                  var sumPctCqiLte7: JFloat = null
                ) extends Serializable