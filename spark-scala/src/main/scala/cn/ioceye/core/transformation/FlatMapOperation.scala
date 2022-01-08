package cn.ioceye.core.transformation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlatMapOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setMaster("local")
               .setAppName("FlatMapOperation")
        
    val sc = new SparkContext(conf)
        
    val lines = Array("hello word", "welcome to here")
    val linesRDD = sc.parallelize(lines, 3)
    
    val wordsRDD = linesRDD.map(line => line.split(" "))
    
    wordsRDD.foreach(word => println(word))
  }
}