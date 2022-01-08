package cn.ioceye.core.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FilterOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("FilterOperation")
        
    val sc = new SparkContext(conf)
        
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 3)
    
    val numberMap = numberRDD.filter(number => (number % 2 == 0))
    
    numberMap.foreach(number => println(number))
  }
}