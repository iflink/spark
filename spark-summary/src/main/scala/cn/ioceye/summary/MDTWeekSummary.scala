package cn.ioceye.summary

import java.text.SimpleDateFormat

import cn.ioceye.counter.mdtweek.{MDTDay, Nei}
import cn.ioceye.utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-submit --class cn.ioceye.summary.MDTWeekSummary --name mdtweek-summary --master yarn --dev-mode cluster
  * --executor-memory 6G --driver-memory 2G --num-executors 10 --executor-cores 2 UWaySpark-1.0-SNAPSHOT.jar 20190411
  * @author guojl
  */
object MDTWeekSummary {
  val table: String = "clt_mro_gc_day_l"
  val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      throw new RuntimeException("args(0) of arguments is null, which is data time of task")
    }

    val sparkConf = new SparkConf()
    // config
    ConfigUtil.setConfig(sparkConf)

    // serializer
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[MDTDay], classOf[Nei]))

    val dataTime = dayFormat.parse(args(0))
    val dataDotTime: DotTime = TimeUtil.currentWeekOfMonday(dataTime)
    val weekPaths: Array[String] = UWayImpalaHome.getWeekAbsolutePath0(dataTime, table)

    for(weekPath <- weekPaths) {
      println("summary impala path: " +  weekPath)
    }

    val sparkSession: SparkSession = SparkUtil.getSparkSession(sparkConf)
    val sparkContext: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext

    val broadCast: Broadcast[DotTime] = sparkContext.broadcast(dataDotTime)

    val mdtDayDF = sqlContext.read.load(weekPaths: _*)

    // rdd transform
    val mdtDayRDD = mdtDayDF.rdd.mapPartitions(iter => MDTWeekFunction.convertMdtByRow(iter))

    // reduce by key, and merge
    val mergeMdtRDD = mdtDayRDD.reduceByKey((mdt, other) => MDTWeekFunction.reduceByKey(mdt, other))
    mergeMdtRDD.persist(StorageLevel.DISK_ONLY)
    // grid
    val mdtByGridRDD = mergeMdtRDD.mapPartitions(MDTWeekFunction.convertByGrid)
    val mdtGroupByGridRDD = mdtByGridRDD.groupByKey(200)
    // convert to row
    val mdtGroupByGridRowRDD = mdtGroupByGridRDD.mapPartitions(iter => {
      val monday: DotTime = broadCast.value
      MDTWeekFunction.convertToRow(iter, monday)
    })

    // repartition
    val gridRowDF = sqlContext.createDataFrame(mdtGroupByGridRowRDD, MDTDay.getSchema())

    gridRowDF.write.mode(SaveMode.Overwrite).format("parquet")
      .partitionBy("year", "month", "day").save("/user/hive/warehouse/mdt_week")

    // clear
    broadCast.destroy()

    // stop
    sparkSession.stop()
  }
}