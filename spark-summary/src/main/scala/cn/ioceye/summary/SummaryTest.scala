package cn.ioceye.summary

import java.text.SimpleDateFormat

/**
  * spark-submit --class cn.ioceye.summary.MDTWeekSummary --name mdtweek-summary --master yarn --dev-mode cluster
  * --executor-memory 6G --driver-memory 2G --num-executors 10 --executor-cores 2 UWaySpark-1.0-SNAPSHOT.jar
  * @author guojl
  */
object SummaryTest {
  val table: String = "clt_mro_gc_day_l"
  val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {
    import java.util.ResourceBundle
    val resource: ResourceBundle = ResourceBundle.getBundle("config")
    val keys = resource.getKeys
    while(keys.hasMoreElements) {
      val key = keys.nextElement()
      println(key + " = " + resource.getString(key))
    }
  }
}