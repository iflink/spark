package cn.ioceye.core.sorted

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SencondarySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("SencondarySort")
               .setMaster("local")
               
    val sc = new SparkContext(conf)
    
    val lineRDD = sc.textFile("hdfs://master:9000/user/spark/data/sort.txt", 3)
    val sencondarySortKeyRDD = lineRDD.map(line => {
      val words = line.split(" ")
      (SencondarySortKey(words(0).toInt, words(1).toInt), line)
    })
    val sortedRDD = sencondarySortKeyRDD.sortByKey(true, 3)
    sortedRDD.foreach(sorted => println(sorted._1.first + " " + sorted._1.second))
    
    sc.stop()
  }
}