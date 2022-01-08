package cn.ioceye.core.started

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("ParallelizeCollection")
               
    val sc   = new SparkContext(conf)
    
    val numbers   = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numnerRDD = sc.parallelize(numbers, 3)
    
    val result = numnerRDD.reduce(_ + _)
    
    println("result = " + result)
  }
}