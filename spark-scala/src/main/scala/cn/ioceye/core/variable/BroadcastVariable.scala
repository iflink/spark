package cn.ioceye.core.variable

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("BroadcastVariable")
               
    val sc = new SparkContext(conf)
    
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 3)
    
    val factor:Int = 3
    val factorBroadcast = sc.broadcast(factor)
    
    val tripleRDD = numberRDD.map(n => n * factorBroadcast.value)
    
    tripleRDD.foreach(ele => println("number = " + ele))
    
    sc.stop()
  }
}