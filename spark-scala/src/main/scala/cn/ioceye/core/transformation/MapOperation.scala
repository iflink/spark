package cn.ioceye.core.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapOperation {
  def main(args: Array[String]): Unit = {
    def conf = new SparkConf()
               .setMaster("local")
               .setAppName("FlatMapOperation")
        
    def sc = new SparkContext(conf)
        
    def numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 3)
    
    val numberMap = numberRDD.map(number => number * 2)
    
    numberMap.foreach(number => println(number))
  }
}