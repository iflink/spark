package cn.ioceye.core.advance

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object GroupTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GroupTop3")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile("hdfs://master:9000/user/spark/data/groupsort.txt", 3)
    val wordRDD = lineRDD.map(line => {
      val words = line.split(" ")
      (words(0), words(1).toInt)
    })
    val groupByClassRDD = wordRDD.groupByKey()
    
    val classTop3RDD = groupByClassRDD.map(clazz => {
      val top3 = new ArrayBuffer[Int]
      for (score <- clazz._2) {
        if (top3.size < 3) {
          top3 += score
        } 
        else {
          var minIndex=0
          var minValue=top3(0)
          for (i <- 0 until 3) {
            if(minValue > top3(i)) {
              minValue = top3(i)
              minIndex = i
            }
          }
          if(minValue < score) {
            top3(minIndex) = score
          }
        }
      }
      (clazz._1, top3)
    })
    
    classTop3RDD.foreach(clazz => println(clazz._1 + " = " + clazz._2))
  }
}