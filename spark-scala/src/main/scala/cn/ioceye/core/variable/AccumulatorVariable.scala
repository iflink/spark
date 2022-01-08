package cn.ioceye.core.variable

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("AccumulatorVariable")
    val sc = new SparkContext(conf)
    val accumulator = sc.accumulator(0)
    
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 3)
    
    numberRDD.foreach(number => accumulator += number)
    
    println("accumulator = " + accumulator.value)
  }
}