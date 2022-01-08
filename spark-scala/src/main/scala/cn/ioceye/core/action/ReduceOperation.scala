package cn.ioceye.core.action

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ReduceOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("ReduceOperation")
               
    val sc = new SparkContext(conf)
    val numberArr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    
    val numberRDD = sc.parallelize(numberArr, 3);
    val result = numberRDD.reduce((a, b) => a+ b)
    println("reduce result = " + result)
  }
}